
"""
Read-only schema contract inspection for the Zomato Lakehouse.

Compares:
1. Bronze source columns against YAML source_column mappings.
2. Silver business columns against canonical column names.
3. Silver data types against canonical data types.
4. Expected versus actual Spark nullability metadata.

Does not:
- Read or display business records.
- Modify tables or YAML contracts.
- Execute arbitrary SQL.
- Treat operational metadata as business-column drift.
- Claim that nullable metadata proves actual NULL values.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional


DATASETS = {
    "food": {
        "bronze_table": "zomato_dev.bronze.food",
        "silver_table": "zomato_dev.silver.food",
    },
    "menu": {
        "bronze_table": "zomato_dev.bronze.menu",
        "silver_table": "zomato_dev.silver.menu",
    },
    "order_items": {
        "bronze_table": "zomato_dev.bronze.order_items",
        "silver_table": "zomato_dev.silver.order_items",
    },
    "orders": {
        "bronze_table": "zomato_dev.bronze.orders",
        "silver_table": "zomato_dev.silver.orders",
    },
    "restaurant": {
        "bronze_table": "zomato_dev.bronze.restaurant",
        "silver_table": "zomato_dev.silver.restaurant",
    },
    "reviews": {
        "bronze_table": "zomato_dev.bronze.reviews",
        "silver_table": "zomato_dev.silver.reviews",
    },
    "users": {
        "bronze_table": "zomato_dev.bronze.users",
        "silver_table": "zomato_dev.silver.users",
    },
}


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCHEMA_CONTRACT_DIR = (
    PROJECT_ROOT / "schemas" / "silver_canonical_schema"
)


SENSITIVE_COLUMN_NAMES = {
    "password",
    "passwd",
    "secret",
    "token",
    "access_key",
    "private_key",
    "ssn",
}


def _normalize_type(data_type: Any) -> str:
    """Normalize SQL/Spark data-type names for comparison."""
    return (
        str(data_type)
        .strip()
        .lower()
        .replace(" ", "")
    )


def _safe_column_name(column_name: str) -> str:
    """Prevent sensitive column names from entering agent output."""
    if column_name.lower() in SENSITIVE_COLUMN_NAMES:
        return "[REDACTED_SENSITIVE_COLUMN]"

    return column_name


def _read_contract(dataset: str) -> Dict[str, Any]:
    """Load and validate one canonical YAML contract."""

    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is unavailable in this Python environment."
        ) from exc

    contract_path = (
        SCHEMA_CONTRACT_DIR / f"{dataset}.yml"
    )

    if not contract_path.is_file():
        raise FileNotFoundError(
            f"Canonical contract not found for dataset '{dataset}'."
        )

    contract = yaml.safe_load(
        contract_path.read_text(encoding="utf-8")
    )

    if not isinstance(contract, dict):
        raise ValueError("YAML contract must contain a mapping.")

    if contract.get("dataset") != dataset:
        raise ValueError(
            "Contract dataset name does not match requested dataset."
        )

    expected_tables = DATASETS[dataset]

    if contract.get("source_table") != expected_tables["bronze_table"]:
        raise ValueError(
            "Contract source_table does not match the approved Bronze table."
        )

    if contract.get("target_table") != expected_tables["silver_table"]:
        raise ValueError(
            "Contract target_table does not match the approved Silver table."
        )

    columns = contract.get("columns")

    if not isinstance(columns, list) or not columns:
        raise ValueError(
            "Contract must contain a non-empty columns list."
        )

    seen_names = set()

    for column in columns:

        if not isinstance(column, dict):
            raise ValueError("Every contract column must be a mapping.")

        name = column.get("name")
        data_type = column.get("data_type")

        if not isinstance(name, str) or not name.strip():
            raise ValueError("Contract contains an invalid column name.")

        if name in seen_names:
            raise ValueError(
                f"Duplicate canonical column definition: {name}"
            )

        seen_names.add(name)

        if not isinstance(data_type, str) or not data_type.strip():
            raise ValueError(
                f"Missing data_type in canonical column: {name}"
            )

        if not isinstance(column.get("nullable"), bool):
            raise ValueError(
                f"Invalid nullable definition in canonical column: {name}"
            )

        source_column = column.get("source_column")
        derived_from = column.get("derived_from")
        derived = column.get("derived", False)

        if (
            source_column is None
            and not derived_from
            and not derived
        ):
            raise ValueError(
                f"Column '{name}' has no source mapping or derived definition."
            )

    return contract


def _inspect_dataset(
    spark: Any,
    dataset: str,
) -> Dict[str, Any]:
    """Compare one dataset's actual schemas with its YAML contract."""

    contract = _read_contract(dataset)
    table_config = DATASETS[dataset]

    bronze_table = table_config["bronze_table"]
    silver_table = table_config["silver_table"]

    # Schema access only. No business records are collected.
    bronze_schema = spark.table(bronze_table).schema
    silver_schema = spark.table(silver_table).schema

    bronze_fields = {
        field.name: field
        for field in bronze_schema.fields
    }

    silver_fields = {
        field.name: field
        for field in silver_schema.fields
    }

    canonical_columns = contract["columns"]

    expected_silver_names = {
        column["name"]
        for column in canonical_columns
    }

    expected_source_names = {
        column["source_column"]
        for column in canonical_columns
        if isinstance(column.get("source_column"), str)
        and column["source_column"].strip()
    }

    # 1. Check expected Bronze source-column mappings.
    missing_bronze_columns = []

    for column in canonical_columns:
        source_column = column.get("source_column")

        if source_column and source_column not in bronze_fields:
            missing_bronze_columns.append({
                "canonical_column": column["name"],
                "expected_source_column": source_column,
            })

    # 2. Check expected Silver business columns.
    missing_silver_columns = sorted(
        expected_silver_names - set(silver_fields)
    )

    # 3. Ignore underscore-prefixed operational/system columns.
    actual_silver_business_names = {
        name
        for name in silver_fields
        if not name.startswith("_")
    }

    unexpected_silver_columns = sorted(
        actual_silver_business_names - expected_silver_names
    )

    # 4. Compare canonical and actual Silver data types.
    data_type_mismatches = []
    nullability_metadata_mismatches = []

    for column in canonical_columns:

        canonical_name = column["name"]

        if canonical_name not in silver_fields:
            continue

        actual_field = silver_fields[canonical_name]

        expected_type = _normalize_type(column["data_type"])
        actual_type = _normalize_type(
            actual_field.dataType.simpleString()
        )

        if expected_type != actual_type:
            data_type_mismatches.append({
                "column": canonical_name,
                "expected_type": expected_type,
                "actual_type": actual_type,
            })

        expected_nullable = column["nullable"]
        actual_nullable = actual_field.nullable

        if expected_nullable != actual_nullable:
            nullability_metadata_mismatches.append({
                "column": canonical_name,
                "expected_nullable": expected_nullable,
                "actual_nullable": actual_nullable,
            })

    # 5. Report Bronze source columns not mapped into canonical Silver.
    # Underscore-prefixed fields are treated as ingestion metadata.
    unmapped_bronze_columns = sorted(
        name
        for name in bronze_fields
        if (
            not name.startswith("_")
            and name not in expected_source_names
        )
    )

    redacted_unmapped_columns = [
        _safe_column_name(name)
        for name in unmapped_bronze_columns
    ]

    sensitive_unmapped_count = sum(
        1
        for name in unmapped_bronze_columns
        if name.lower() in SENSITIVE_COLUMN_NAMES
    )

    # 6. Record derived-column definitions without claiming that
    # transformation logic itself has been executed or verified.
    derived_columns = []

    for column in canonical_columns:
        if column.get("derived") or column.get("derived_from"):
            derived_columns.append({
                "column": column["name"],
                "derived_from": column.get("derived_from"),
                "transformation_defined": bool(
                    column.get("transformation")
                ),
            })

    has_schema_drift = any([
        missing_bronze_columns,
        missing_silver_columns,
        unexpected_silver_columns,
        data_type_mismatches,
    ])

    has_metadata_warnings = any([
        nullability_metadata_mismatches,
        unmapped_bronze_columns,
    ])

    if has_schema_drift:
        status = "DRIFT_DETECTED"
    elif has_metadata_warnings:
        status = "OK_WITH_WARNINGS"
    else:
        status = "OK"

    return {
        "status": status,
        "dataset": dataset,
        "contract_file": (
            f"schemas/silver_canonical_schema/{dataset}.yml"
        ),
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "expected_business_column_count": len(
            expected_silver_names
        ),
        "actual_silver_business_column_count": len(
            actual_silver_business_names
        ),
        "missing_bronze_source_columns": missing_bronze_columns,
        "missing_silver_columns": missing_silver_columns,
        "unexpected_silver_columns": unexpected_silver_columns,
        "data_type_mismatches": data_type_mismatches,
        "nullability_metadata_mismatches": (
            nullability_metadata_mismatches
        ),
        "unmapped_bronze_columns": redacted_unmapped_columns,
        "sensitive_unmapped_source_column_count": (
            sensitive_unmapped_count
        ),
        "derived_columns": derived_columns,
        "data_values_scanned": False,
        "null_values_validated": False,
    }


def inspect_schema_changes(
    spark: Any,
    *,
    dataset: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compare approved canonical schema contracts with Bronze/Silver schemas.

    Args:
        spark: Active SparkSession.
        dataset: Optional dataset name. When omitted, inspect all seven.

    Returns:
        Structured schema comparison evidence.
    """

    if dataset is not None:

        if not isinstance(dataset, str) or not dataset.strip():
            return {
                "status": "INVALID_ARGUMENT",
                "error": "dataset must be a non-empty string.",
            }

        dataset = dataset.strip().lower()

        if dataset not in DATASETS:
            return {
                "status": "INVALID_ARGUMENT",
                "error": "Unsupported dataset.",
                "allowed_datasets": sorted(DATASETS),
            }

        selected_datasets = [dataset]

    else:
        selected_datasets = sorted(DATASETS)

    results = {}
    errors = []

    for name in selected_datasets:

        try:
            results[name] = _inspect_dataset(
                spark=spark,
                dataset=name,
            )

            if results[name]["status"] in {
                "ERROR",
                "DEPENDENCY_MISSING",
                "INVALID_CONTRACT",
            }:
                errors.append(name)

        except RuntimeError as exc:
            message = str(exc)

            if "PyYAML is unavailable" in message:
                error_status = "DEPENDENCY_MISSING"
            else:
                error_status = "ERROR"

            results[name] = {
                "status": error_status,
                "dataset": name,
                "error_type": type(exc).__name__,
                "error_message": message[:500],
            }

            errors.append(name)

        except (FileNotFoundError, ValueError) as exc:
            results[name] = {
                "status": "INVALID_CONTRACT",
                "dataset": name,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }

            errors.append(name)

        except Exception as exc:
            results[name] = {
                "status": "ERROR",
                "dataset": name,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }

            errors.append(name)

    statuses = [
        result["status"]
        for result in results.values()
    ]

    if errors:
        if len(errors) == len(selected_datasets):
            overall_status = "ERROR"
        else:
            overall_status = "PARTIAL"

    elif "DRIFT_DETECTED" in statuses:
        overall_status = "DRIFT_DETECTED"

    elif "OK_WITH_WARNINGS" in statuses:
        overall_status = "OK_WITH_WARNINGS"

    else:
        overall_status = "OK"

    return {
        "status": overall_status,
        "tool_name": "inspect_schema_changes",
        "dataset_filter": dataset,
        "inspected_dataset_count": (
            len(results) - len(errors)
        ),
        "failed_datasets": errors,
        "datasets": results,
        "comparison_scope": {
            "bronze": "SOURCE_COLUMN_MAPPING",
            "silver": "CANONICAL_NAMES_AND_DATA_TYPES",
            "nullability": "METADATA_COMPARISON_ONLY",
            "operational_metadata": "EXCLUDED_FROM_BUSINESS_DRIFT",
        },
        "limitations": [
            "Schema inspection does not scan business records.",
            "Spark nullability metadata does not prove actual NULL values.",
            "Unmapped Bronze columns are reported but are not automatically classified as drift.",
            "Derived transformation logic is identified but not independently executed.",
            "No historical schema registry is available to establish when a change occurred.",
        ],
    }

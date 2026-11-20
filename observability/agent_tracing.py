"""MLflow tracing adapter for the existing registered Zomato investigation.

The registered workflow remains the execution authority. This adapter only
wraps the exact governed tool callables returned by its existing registry and
optionally forwards durable evidence checkpoints to a caller-supplied callback.
"""

from __future__ import annotations

from functools import wraps
from time import perf_counter
from typing import Any, Callable, Dict, Optional

from observability.config import configure_experiment
from observability.tracing import (
    child_span,
    summarize_investigation_report,
    summarize_tool_inputs,
    summarize_tool_output,
    tracing_session,
)


def _instrument_governed_tool(
    tool_name: str,
    tool_function: Callable[..., Any],
) -> Callable[..., Any]:
    """Wrap one existing governed tool with a safe MLflow child span."""
    if not callable(tool_function):
        raise TypeError(f"Governed tool {tool_name!r} is not callable.")

    span_type = "RETRIEVER" if tool_name == "search_runbooks" else "TOOL"

    @wraps(tool_function)
    def traced_tool(*args: Any, **kwargs: Any) -> Any:
        safe_inputs = summarize_tool_inputs(tool_name, kwargs)
        started = perf_counter()

        with child_span(
            name=f"tool.{tool_name}",
            span_type=span_type,
            inputs=safe_inputs,
            attributes={"zomato.tool_name": tool_name},
        ) as span:
            try:
                result = tool_function(*args, **kwargs)
            except Exception as exc:
                elapsed_ms = round((perf_counter() - started) * 1000, 3)
                if span is not None:
                    span.set_outputs(
                        {
                            "tool_name": tool_name,
                            "status": "EXCEPTION",
                            "exception_type": type(exc).__name__,
                        }
                    )
                    span.set_attribute("zomato.tool.duration_ms", elapsed_ms)
                raise

            elapsed_ms = round((perf_counter() - started) * 1000, 3)
            if span is not None:
                span.set_outputs(summarize_tool_output(tool_name, result))
                span.set_attribute("zomato.tool.duration_ms", elapsed_ms)
                if isinstance(result, dict):
                    span.set_attribute(
                        "zomato.tool.status",
                        str(result.get("status", "UNKNOWN")),
                    )
            return result

    return traced_tool


def run_traced_registered_investigation(
    *,
    run_id: int,
    workspace_client: Any,
    spark: Any,
    runbook_top_k: int = 5,
    request_id: Optional[str] = None,
    request_source: str = "DAY29_OBSERVABILITY",
    persist: bool = False,
    evidence_persist_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Run the registered workflow inside one MLflow trace.

    ``persist`` controls the workflow's request/report persistence. It is
    independent from ``evidence_persist_callback``. When a callback is
    supplied, the workflow checkpoints each sanitized governed-tool result
    before it proceeds to the next investigation phase, even when
    ``persist=False``.

    The callback must accept keyword arguments supplied by the workflow plus
    ``mlflow_trace_id``. A suitable callback can be created with
    ``operations.evidence_snapshots.build_evidence_persist_callback``.
    """
    if evidence_persist_callback is not None and not callable(
        evidence_persist_callback
    ):
        raise TypeError("evidence_persist_callback must be callable when supplied.")

    import mlflow

    from operations import investigation_workflow as workflow

    experiment = configure_experiment()

    mlflow_trace_id = None
    mlflow_trace_id_available_during_checkpoints = False
    with tracing_session():
        with mlflow.start_span(
            name="zomato.incident_investigation",
            span_type="AGENT",
        ) as agent_span:
            # Capture the trace identity before the first governed tool runs so
            # every durable evidence snapshot can carry the same correlation ID.
            mlflow_trace_id = getattr(agent_span, "trace_id", None)
            mlflow_trace_id_available_during_checkpoints = bool(mlflow_trace_id)
            agent_span.set_inputs(
                {
                    "run_id": run_id,
                    "request_id": request_id,
                    "request_source": request_source,
                    "runbook_top_k": runbook_top_k,
                    "persist": persist,
                    "evidence_checkpointing": evidence_persist_callback is not None,
                    "workflow_version": workflow.WORKFLOW_VERSION,
                    "rag_model_endpoint": workflow.DEFAULT_RAG_MODEL_ENDPOINT,
                    "experiment_id": experiment.experiment_id,
                }
            )
            agent_span.set_attribute(
                "zomato.workflow_version",
                workflow.WORKFLOW_VERSION,
            )
            agent_span.set_attribute(
                "zomato.model.endpoint",
                workflow.DEFAULT_RAG_MODEL_ENDPOINT,
            )
            agent_span.set_attribute(
                "zomato.evidence_checkpointing.enabled",
                evidence_persist_callback is not None,
            )
            if mlflow_trace_id:
                agent_span.set_attribute(
                    "zomato.mlflow_trace_id",
                    str(mlflow_trace_id),
                )

            traced_evidence_callback = None
            if evidence_persist_callback is not None:
                def traced_evidence_callback(**kwargs: Any) -> Any:
                    callback_kwargs = dict(kwargs)
                    callback_kwargs["mlflow_trace_id"] = mlflow_trace_id
                    return evidence_persist_callback(**callback_kwargs)

            report = workflow.run_registered_investigation(
                run_id=run_id,
                workspace_client=workspace_client,
                spark=spark,
                runbook_top_k=runbook_top_k,
                request_id=request_id,
                request_source=request_source,
                persist=persist,
                tool_callable_wrapper=_instrument_governed_tool,
                evidence_persist_callback=traced_evidence_callback,
            )

            if not isinstance(report, dict):
                raise TypeError(
                    "The registered investigation returned a non-dictionary report."
                )

            agent_span.set_outputs(summarize_investigation_report(report))
            workflow_trace_id = report.get("trace_id")
            if workflow_trace_id:
                agent_span.set_attribute(
                    "zomato.workflow_trace_id",
                    str(workflow_trace_id),
                )

    if not mlflow_trace_id:
        try:
            mlflow_trace_id = mlflow.get_last_active_trace_id(thread_local=True)
        except TypeError:
            # Compatibility fallback for older MLflow 3 minor versions.
            mlflow_trace_id = mlflow.get_last_active_trace_id()

    report["observability"] = {
        "mlflow_trace_id": mlflow_trace_id,
        "experiment_id": experiment.experiment_id,
        "experiment_name": experiment.name,
        "trace_storage": "MLFLOW_EXPERIMENT",
        "evidence_checkpointing": {
            "enabled": evidence_persist_callback is not None,
            "mlflow_trace_id_available_during_checkpoints": (
                mlflow_trace_id_available_during_checkpoints
            ),
        },
    }
    return report

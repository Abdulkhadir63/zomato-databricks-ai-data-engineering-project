
"""
Agent-facing read-only runbook search contract.

Delegates retrieval to the existing production AI Search implementation.
Does not create another index or duplicate retrieval logic.

Response statuses:
- OK
- NO_MATCH
- INVALID_ARGUMENT
- MALFORMED_OUTPUT
- TIMEOUT
- ERROR
"""

from typing import Any, Dict, Optional

from ai_search.search import search_runbooks as _search_runbooks


MAX_TOP_K = 10


class MalformedSearchOutputError(Exception):
    """Raised when AI Search returns an invalid result structure."""


def search_runbooks(
    query: str,
    *,
    top_k: int = 5,
    additional_filters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Search approved project-production runbooks.

    Args:
        query: Natural-language search query.
        top_k: Maximum number of runbook chunks to retrieve (1..10).
        additional_filters: Optional approved metadata filters.

    Returns:
        Structured response containing retrieved evidence or an
        explicit error status.
    """

    # 1. Validate query.
    if not isinstance(query, str) or not query.strip():
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "search_runbooks",
            "error": "query must be a non-empty string.",
        }

    # 2. Validate retrieval limit.
    if (
        isinstance(top_k, bool)
        or not isinstance(top_k, int)
        or not 1 <= top_k <= MAX_TOP_K
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "search_runbooks",
            "error": (
                f"top_k must be an integer between 1 and {MAX_TOP_K}."
            ),
        }

    # 3. Validate filter container.
    if (
        additional_filters is not None
        and not isinstance(additional_filters, dict)
    ):
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "search_runbooks",
            "error": "additional_filters must be a dictionary.",
        }

    try:
        # 4. Delegate to the existing production retrieval implementation.
        results = _search_runbooks(
            query=query.strip(),
            top_k=top_k,
            additional_filters=additional_filters,
        )

        # 5. Validate the retrieval response structure.
        if not isinstance(results, list):
            raise MalformedSearchOutputError(
                "AI Search returned a non-list result."
            )

        if len(results) > top_k:
            raise MalformedSearchOutputError(
                "AI Search returned more results than requested."
            )

        validated_results = []

        for index, result in enumerate(results):

            if not isinstance(result, dict):
                raise MalformedSearchOutputError(
                    f"Search result {index} is not a dictionary."
                )

            chunk_id = result.get("chunk_id")
            chunk_text = result.get("chunk_text")

            if not isinstance(chunk_id, str) or not chunk_id.strip():
                raise MalformedSearchOutputError(
                    f"Search result {index} has no valid chunk_id."
                )

            if not isinstance(chunk_text, str) or not chunk_text.strip():
                raise MalformedSearchOutputError(
                    f"Search result {index} has no valid chunk_text."
                )

            validated_results.append(result)

        # 6. Return a consistent agent-facing response.
        return {
            "status": "OK" if validated_results else "NO_MATCH",
            "tool_name": "search_runbooks",
            "retrieval_source": "PROJECT_PRODUCTION_AI_SEARCH",
            "requested_top_k": top_k,
            "retrieved_count": len(validated_results),
            "evidence": validated_results,
            "evidence_scope": "APPROVED_PROJECT_RUNBOOKS",
            "timeout_policy": (
                "Timeout exceptions are normalized, but execution "
                "timeouts are not enforced by this adapter. "
                "Bounded execution is deferred to Day 27."
            ),
        }

    except MalformedSearchOutputError as exc:
        return {
            "status": "MALFORMED_OUTPUT",
            "tool_name": "search_runbooks",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
        }

    except TimeoutError as exc:
        return {
            "status": "TIMEOUT",
            "tool_name": "search_runbooks",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
        }

    except (TypeError, ValueError) as exc:
        return {
            "status": "INVALID_ARGUMENT",
            "tool_name": "search_runbooks",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
        }

    except Exception as exc:
        return {
            "status": "ERROR",
            "tool_name": "search_runbooks",
            "error_type": type(exc).__name__,
            "error": str(exc)[:500],
        }

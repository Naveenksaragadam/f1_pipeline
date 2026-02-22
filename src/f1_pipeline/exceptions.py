# src/f1_pipeline/exceptions.py
"""
Custom exception hierarchy for the F1 Data Pipeline.

Using a typed hierarchy lets call sites distinguish between errors that should
halt the run immediately (DataCorruptionError) vs. errors that are transient
and should be retried or counted-and-skipped.
"""


class F1PipelineError(Exception):
    """Base class for all pipeline-specific errors."""


class DataCorruptionError(F1PipelineError):
    """
    Raised when an API response claims records exist but the payload is empty.

    This indicates a silent API bug, not a transient network issue, so it must
    propagate and halt the run rather than being counted as a skippable error.
    """


class TransientAPIError(F1PipelineError):
    """
    Raised for recoverable API errors that should be retried.

    Currently a placeholder — tenacity handles retry logic directly on
    fetch_page via the @retry decorator.
    """

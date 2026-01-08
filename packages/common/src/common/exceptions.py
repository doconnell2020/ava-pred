"""Custom exceptions for the ava-pred ETL pipeline."""


class AvaError(Exception):
    """Base exception for all ava-pred errors."""


class ExtractionError(AvaError):
    """Raised when data extraction fails."""

    def __init__(self, message: str, url: str | None = None) -> None:
        self.url = url
        super().__init__(message)


class TransformError(AvaError):
    """Raised when data transformation fails."""

    def __init__(self, message: str, source: str | None = None) -> None:
        self.source = source
        super().__init__(message)


class ValidationError(AvaError):
    """Raised when data validation fails."""

    def __init__(self, message: str, field: str | None = None) -> None:
        self.field = field
        super().__init__(message)

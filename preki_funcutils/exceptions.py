from typing import Optional
from . import status
from .logger import LogLevel


class PrekiException(Exception):

    def __init__(
        self,
        message: str,
        error_code: str = None,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        data: Optional[dict] = None,
        log_event: Optional[str] = None,
        log_data: dict = {},
        log_level: LogLevel = LogLevel.ERROR,
        force_error: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.data = data
        self.force_error = force_error
        self.log_event = log_event
        self.log_data = log_data
        self.log_level = log_level

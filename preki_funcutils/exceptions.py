from typing import Optional
from . import status


class PrekiException(Exception):

    def __init__(
        self,
        message: str,
        error_code: str = None,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        data: Optional[dict] = None,
        force_error: bool = False,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.data = data
        self.force_error = force_error

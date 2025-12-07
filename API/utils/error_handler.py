"""
Error handling utilities
"""
import logging
from typing import Dict, Any
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base API error class"""
    def __init__(self, message: str, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class DatabaseError(APIError):
    """Database-related errors"""
    def __init__(self, message: str):
        super().__init__(message, status.HTTP_500_INTERNAL_SERVER_ERROR)


class ValidationError(APIError):
    """Validation errors"""
    def __init__(self, message: str):
        super().__init__(message, status.HTTP_400_BAD_REQUEST)


class NotFoundError(APIError):
    """Resource not found errors"""
    def __init__(self, message: str):
        super().__init__(message, status.HTTP_404_NOT_FOUND)


def handle_exception(error: Exception) -> HTTPException:
    """
    Convert exceptions to HTTP exceptions
    
    Args:
        error: Exception to handle
        
    Returns:
        HTTPException with appropriate status code and message
    """
    if isinstance(error, APIError):
        logger.error(f"API Error: {error.message}")
        return HTTPException(status_code=error.status_code, detail=error.message)
    elif isinstance(error, ValueError):
        logger.error(f"Validation Error: {str(error)}")
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error))
    else:
        logger.exception("Unexpected error occurred")
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred. Please try again later."
        )


def create_error_response(message: str, status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR) -> Dict[str, Any]:
    """Create standardized error response"""
    return {
        "error": True,
        "message": message,
        "status_code": status_code
    }


"""
Error handling utilities for batch calling operations.

This module provides specialized exception classes and error handling
utilities for robust batch calling operations.
"""

import time
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BatchError(Exception):
    """Base exception for batch operations."""
    pass


class RateLimitError(BatchError):
    """Raised when rate limit is hit."""

    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


class NetworkError(BatchError):
    """Raised when network-related errors occur."""
    pass


class ContractError(BatchError):
    """Raised when contract-related errors occur."""
    pass


class ValidationError(BatchError):
    """Raised when input validation fails."""
    pass


class ProviderRotator:
    """
    Manages Web3 provider rotation for improved reliability.

    Automatically switches to backup providers when primary fails,
    implementing circuit breaker pattern for failed providers.
    """

    def __init__(self, providers: Dict[str, Any], failure_threshold: int = 3):
        """
        Initialize provider rotator.

        Args:
            providers: Dictionary of provider names to Web3 instances
            failure_threshold: Number of failures before marking provider as failed
        """
        self.providers = providers
        self.provider_names = list(providers.keys())
        self.current_index = 0
        self.failure_threshold = failure_threshold
        self.failure_counts = {name: 0 for name in self.provider_names}
        self.last_failure_times = {name: 0 for name in self.provider_names}
        self.circuit_breaker_timeout = 60.0  # Reset failed providers after 60 seconds

    def get_current_provider(self) -> Any:
        """Get the current active provider."""
        if not self.provider_names:
            raise BatchError("No providers available")

        return self.providers[self.provider_names[self.current_index]]

    def get_current_provider_name(self) -> str:
        """Get the name of current active provider."""
        if not self.provider_names:
            raise BatchError("No providers available")

        return self.provider_names[self.current_index]

    def mark_failure(self, provider_name: Optional[str] = None):
        """
        Mark a provider as failed and rotate to next available.

        Args:
            provider_name: Name of failed provider (uses current if None)
        """
        if provider_name is None:
            provider_name = self.get_current_provider_name()

        self.failure_counts[provider_name] += 1
        self.last_failure_times[provider_name] = time.time()

        logger.warning(
            f"Provider {provider_name} failed "
            f"({self.failure_counts[provider_name]}/{self.failure_threshold})"
        )

        # Rotate to next available provider
        self._rotate_to_next_available()

    def _rotate_to_next_available(self):
        """Rotate to the next available provider."""
        original_index = self.current_index
        attempts = 0

        while attempts < len(self.provider_names):
            self.current_index = (self.current_index + 1) % len(self.provider_names)
            current_name = self.provider_names[self.current_index]

            # Check if provider is available (not in circuit breaker state)
            if self._is_provider_available(current_name):
                logger.info(f"Rotated to provider: {current_name}")
                return

            attempts += 1

        # If we get here, all providers are failed
        logger.error("All providers are currently failed")
        # Reset to original position
        self.current_index = original_index

    def _is_provider_available(self, provider_name: str) -> bool:
        """
        Check if a provider is available (not in circuit breaker state).

        Args:
            provider_name: Name of provider to check

        Returns:
            True if provider is available
        """
        failure_count = self.failure_counts[provider_name]

        # Provider is available if it hasn't exceeded failure threshold
        if failure_count < self.failure_threshold:
            return True

        # Or if enough time has passed since last failure (circuit breaker reset)
        time_since_failure = time.time() - self.last_failure_times[provider_name]
        if time_since_failure >= self.circuit_breaker_timeout:
            # Reset failure count
            self.failure_counts[provider_name] = 0
            logger.info(f"Reset circuit breaker for provider: {provider_name}")
            return True

        return False

    def get_available_providers(self) -> list:
        """Get list of currently available provider names."""
        return [
            name for name in self.provider_names
            if self._is_provider_available(name)
        ]


class ErrorHandler:
    """
    Centralized error handling for batch operations.

    Provides classification, logging, and recovery strategies
    for various types of errors encountered during batch calls.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def classify_error(self, error: Exception) -> str:
        """
        Classify an error into a category for appropriate handling.

        Args:
            error: Exception to classify

        Returns:
            Error category string
        """
        error_str = str(error).lower()

        # Rate limiting errors
        if any(keyword in error_str for keyword in ['rate limit', 'too many requests', '429']):
            return 'rate_limit'

        # Network connectivity errors
        if any(keyword in error_str for keyword in ['connection', 'timeout', 'network', 'dns']):
            return 'network'

        # Contract execution errors
        if any(keyword in error_str for keyword in ['revert', 'execution reverted', 'out of gas']):
            return 'contract'

        # Validation errors
        if any(keyword in error_str for keyword in ['invalid', 'bad request', '400']):
            return 'validation'

        return 'unknown'

    def should_retry(self, error: Exception, attempt: int, max_retries: int) -> bool:
        """
        Determine if an error should trigger a retry.

        Args:
            error: Exception that occurred
            attempt: Current attempt number (0-based)
            max_retries: Maximum number of retries allowed

        Returns:
            True if operation should be retried
        """
        if attempt >= max_retries:
            return False

        error_category = self.classify_error(error)

        # Don't retry validation errors
        if error_category == 'validation':
            return False

        # Retry network and rate limit errors
        if error_category in ['network', 'rate_limit', 'unknown']:
            return True

        # Don't retry contract errors (they're deterministic)
        if error_category == 'contract':
            return False

        return False

    def get_retry_delay(self, error: Exception, attempt: int) -> float:
        """
        Calculate appropriate retry delay based on error type and attempt.

        Args:
            error: Exception that occurred
            attempt: Current attempt number (0-based)

        Returns:
            Delay in seconds before retry
        """
        error_category = self.classify_error(error)

        # Base exponential backoff
        base_delay = min(2 ** attempt, 60)  # Cap at 60 seconds

        # Rate limit errors get longer delays
        if error_category == 'rate_limit':
            return base_delay * 2

        # Network errors get standard backoff
        if error_category == 'network':
            return base_delay

        # Unknown errors get conservative delay
        return base_delay * 1.5

    def log_error(self, error: Exception, context: Dict[str, Any]):
        """
        Log error with appropriate level and context.

        Args:
            error: Exception to log
            context: Additional context for logging
        """
        error_category = self.classify_error(error)

        log_data = {
            'error_type': type(error).__name__,
            'error_category': error_category,
            'error_message': str(error),
            **context
        }

        # Log validation errors as warnings
        if error_category == 'validation':
            self.logger.warning("Validation error occurred", extra=log_data)
        # Log contract errors as errors
        elif error_category == 'contract':
            self.logger.error("Contract execution failed", extra=log_data)
        # Log rate limit as info (expected)
        elif error_category == 'rate_limit':
            self.logger.info("Rate limit encountered", extra=log_data)
        # Everything else as warning
        else:
            self.logger.warning("Batch operation error", extra=log_data)
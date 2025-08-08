"""OAuth authentication for Databricks."""

import requests
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from dbxsql.settings import DatabricksSettings
from dbxsql.exceptions import AuthenticationError

logger = logging.getLogger(__name__)


class TokenProvider(ABC):
    """Abstract base class for token providers."""

    @abstractmethod
    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get valid access token."""
        ...

    @abstractmethod
    def invalidate_token(self) -> None:
        """Invalidate current token."""
        ...

    @abstractmethod
    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        ...


class OAuthManager(TokenProvider):
    """Manages OAuth authentication for Databricks using client credentials flow."""

    def __init__(self, settings: DatabricksSettings):
        self._settings = settings
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._token_buffer_minutes = 5  # Refresh token 5 minutes before expiry

    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get valid access token, refreshing if necessary.

        Args:
            force_refresh: Force token refresh even if current token is valid

        Returns:
            Valid access token

        Raises:
            AuthenticationError: If authentication fails
        """
        if force_refresh or self._is_token_expired():
            self._refresh_token()

        if not self._access_token:
            raise AuthenticationError("No valid access token available")

        return self._access_token

    def _is_token_expired(self) -> bool:
        """Check if current token is expired or will expire soon."""
        if not self._access_token or not self._token_expiry:
            return True

        # Refresh token before expiry to avoid race conditions
        buffer_time = timedelta(minutes=self._token_buffer_minutes)
        return datetime.now() >= (self._token_expiry - buffer_time)

    def _refresh_token(self) -> None:
        """Refresh OAuth access token using client credentials flow."""
        try:
            token_data = self._request_token()
            self._process_token_response(token_data)
            logger.info("OAuth token obtained successfully")

        except requests.exceptions.RequestException as e:
            error_msg = f"Network error while getting OAuth token: {str(e)}"
            logger.error(error_msg)
            raise AuthenticationError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error during authentication: {str(e)}"
            logger.error(error_msg)
            raise AuthenticationError(error_msg) from e

    def _request_token(self) -> Dict[str, Any]:
        """Make the token request to the OAuth endpoint."""
        token_url = self._settings.get_token_url()

        data = {
            'grant_type': 'client_credentials',
            'scope': self._settings.oauth_scope
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        auth = (self._settings.client_id, self._settings.client_secret)

        logger.info("Requesting OAuth token...")
        response = requests.post(
            token_url,
            data=data,
            auth=auth,
            headers=headers,
            timeout=self._settings.connection_timeout
        )

        if response.status_code != 200:
            error_msg = f"Failed to get OAuth token: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise AuthenticationError(error_msg)

        return response.json()

    def _process_token_response(self, token_data: Dict[str, Any]) -> None:
        """Process the token response and update internal state."""
        if 'access_token' not in token_data:
            raise AuthenticationError("Invalid token response: missing access_token")

        self._access_token = token_data['access_token']

        # Calculate expiry time (default to 1 hour if not provided)
        expires_in = token_data.get('expires_in', 3600)
        self._token_expiry = datetime.now() + timedelta(seconds=expires_in)

    def invalidate_token(self) -> None:
        """Invalidate current token to force refresh on next use."""
        logger.info("Invalidating current token")
        self._access_token = None
        self._token_expiry = None

    def is_authenticated(self) -> bool:
        """Check if currently authenticated with valid token."""
        return self._access_token is not None and not self._is_token_expired()

    def get_token_info(self) -> Dict[str, Any]:
        """Get information about the current token."""
        return {
            'has_token': self._access_token is not None,
            'is_expired': self._is_token_expired(),
            'expires_at': self._token_expiry.isoformat() if self._token_expiry else None,
            'expires_in_seconds': (
                int((self._token_expiry - datetime.now()).total_seconds())
                if self._token_expiry else None
            )
        }
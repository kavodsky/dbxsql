"""Tests for authentication module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import requests

from dbxsql.auth import OAuthManager, TokenProvider
from dbxsql.settings import DatabricksSettings
from dbxsql.exceptions import AuthenticationError


class TestOAuthManager:
    """Test cases for OAuthManager."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings fixture."""
        settings = Mock(spec=DatabricksSettings)
        settings.client_id = "test_client_id"
        settings.client_secret = "test_client_secret"
        settings.oauth_scope = "all-apis"
        settings.connection_timeout = 30
        settings.get_token_url.return_value = "https://test.databricks.com/oidc/v1/token"
        return settings

    @pytest.fixture
    def oauth_manager(self, mock_settings):
        """OAuth manager fixture."""
        return OAuthManager(mock_settings)

    def test_oauth_manager_implements_token_provider(self, oauth_manager):
        """Test that OAuthManager implements TokenProvider protocol."""
        assert isinstance(oauth_manager, TokenProvider)

    @patch('dbxsql.auth.requests.post')
    def test_successful_token_refresh(self, mock_post, oauth_manager, mock_settings):
        """Test successful token refresh."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_access_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response

        token = oauth_manager.get_access_token()

        assert token == 'test_access_token'
        assert oauth_manager.is_authenticated()

        # Verify the request was made correctly
        mock_post.assert_called_once_with(
            'https://test.databricks.com/oidc/v1/token',
            data={
                'grant_type': 'client_credentials',
                'scope': 'all-apis'
            },
            auth=('test_client_id', 'test_client_secret'),
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )

    @patch('dbxsql.auth.requests.post')
    def test_token_not_refreshed_when_valid(self, mock_post, oauth_manager):
        """Test that valid token is not refreshed unnecessarily."""
        # Set up a valid token
        oauth_manager._access_token = 'existing_token'
        oauth_manager._token_expiry = datetime.now() + timedelta(hours=1)

        token = oauth_manager.get_access_token()

        assert token == 'existing_token'
        # Should not make HTTP request
        mock_post.assert_not_called()

    @patch('dbxsql.auth.requests.post')
    def test_force_refresh_token(self, mock_post, oauth_manager):
        """Test force refresh of valid token."""
        # Set up a valid token
        oauth_manager._access_token = 'existing_token'
        oauth_manager._token_expiry = datetime.now() + timedelta(hours=1)

        # Mock new token response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'new_access_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response

        token = oauth_manager.get_access_token(force_refresh=True)

        assert token == 'new_access_token'
        mock_post.assert_called_once()

    @patch('dbxsql.auth.requests.post')
    def test_token_refresh_failure_http_error(self, mock_post, oauth_manager):
        """Test token refresh failure with HTTP error."""
        # Mock failed response
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Invalid client credentials"
        mock_post.return_value = mock_response

        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.get_access_token()

        assert "Failed to get OAuth token: 400" in str(exc_info.value)
        assert "Invalid client credentials" in str(exc_info.value)

    @patch('dbxsql.auth.requests.post')
    def test_token_refresh_network_error(self, mock_post, oauth_manager):
        """Test token refresh failure with network error."""
        # Mock network error
        mock_post.side_effect = requests.exceptions.ConnectionError("Network error")

        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.get_access_token()

        assert "Network error while getting OAuth token" in str(exc_info.value)

    @patch('dbxsql.auth.requests.post')
    def test_token_refresh_unexpected_error(self, mock_post, oauth_manager):
        """Test token refresh failure with unexpected error."""
        # Mock unexpected error
        mock_post.side_effect = ValueError("Unexpected error")

        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.get_access_token()

        assert "Unexpected error during authentication" in str(exc_info.value)

    @patch('dbxsql.auth.requests.post')
    def test_invalid_token_response(self, mock_post, oauth_manager):
        """Test handling of invalid token response."""
        # Mock response without access_token
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'expires_in': 3600}  # Missing access_token
        mock_post.return_value = mock_response

        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.get_access_token()

        assert "Invalid token response: missing access_token" in str(exc_info.value)

    def test_token_expiry_check(self, oauth_manager):
        """Test token expiry checking logic."""
        # No token - should be expired
        assert oauth_manager._is_token_expired()

        # Valid token with future expiry
        oauth_manager._access_token = 'token'
        oauth_manager._token_expiry = datetime.now() + timedelta(hours=1)
        assert not oauth_manager._is_token_expired()

        # Token expiring soon (within buffer time)
        oauth_manager._token_expiry = datetime.now() + timedelta(minutes=3)
        assert oauth_manager._is_token_expired()

        # Expired token
        oauth_manager._token_expiry = datetime.now() - timedelta(minutes=10)
        assert oauth_manager._is_token_expired()

    @patch('dbxsql.auth.requests.post')
    def test_default_token_expiry(self, mock_post, oauth_manager):
        """Test default token expiry when not provided."""
        # Mock response without expires_in
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'access_token': 'test_token'}
        mock_post.return_value = mock_response

        token = oauth_manager.get_access_token()

        assert token == 'test_token'
        # Should set default expiry of 1 hour
        expected_expiry = datetime.now() + timedelta(seconds=3600)
        # Allow some tolerance for test execution time
        assert abs((oauth_manager._token_expiry - expected_expiry).total_seconds()) < 5

    def test_invalidate_token(self, oauth_manager):
        """Test token invalidation."""
        # Set up token
        oauth_manager._access_token = 'token'
        oauth_manager._token_expiry = datetime.now() + timedelta(hours=1)

        assert oauth_manager.is_authenticated()

        oauth_manager.invalidate_token()

        assert not oauth_manager.is_authenticated()
        assert oauth_manager._access_token is None
        assert oauth_manager._token_expiry is None

    def test_is_authenticated(self, oauth_manager):
        """Test authentication status checking."""
        # No token
        assert not oauth_manager.is_authenticated()

        # Valid token
        oauth_manager._access_token = 'token'
        oauth_manager._token_expiry = datetime.now() + timedelta(hours=1)
        assert oauth_manager.is_authenticated()

        # Expired token
        oauth_manager._token_expiry = datetime.now() - timedelta(hours=1)
        assert not oauth_manager.is_authenticated()

    def test_get_token_info(self, oauth_manager):
        """Test getting token information."""
        # No token
        info = oauth_manager.get_token_info()
        assert info['has_token'] is False
        assert info['is_expired'] is True
        assert info['expires_at'] is None
        assert info['expires_in_seconds'] is None

        # Valid token
        expiry_time = datetime.now() + timedelta(hours=1)
        oauth_manager._access_token = 'token'
        oauth_manager._token_expiry = expiry_time

        info = oauth_manager.get_token_info()
        assert info['has_token'] is True
        assert info['is_expired'] is False
        assert info['expires_at'] == expiry_time.isoformat()
        assert isinstance(info['expires_in_seconds'], int)
        assert info['expires_in_seconds'] > 0

    @patch('dbxsql.auth.requests.post')
    def test_get_access_token_no_valid_token_after_refresh(self, mock_post, oauth_manager):
        """Test error when no valid token after refresh attempt."""
        # Mock successful response but somehow token is not set
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'access_token': 'test_token'}
        mock_post.return_value = mock_response

        # Manually set token to None after refresh would normally set it
        def clear_token(*args, **kwargs):
            oauth_manager._access_token = None
            return mock_response

        mock_post.side_effect = clear_token

        with pytest.raises(AuthenticationError) as exc_info:
            oauth_manager.get_access_token()

        assert "No valid access token available" in str(exc_info.value)

    def test_token_buffer_minutes_configuration(self, oauth_manager):
        """Test that token buffer minutes can be configured."""
        # Set token that would normally be valid but within buffer
        oauth_manager._access_token = 'token'
        oauth_manager._token_expiry = datetime.now() + timedelta(minutes=3)

        # Should be considered expired due to buffer
        assert oauth_manager._is_token_expired()

        # Change buffer and test
        oauth_manager._token_buffer_minutes = 1
        assert not oauth_manager._is_token_expired()

    @patch('dbxsql.auth.datetime')
    @patch('dbxsql.auth.requests.post')
    def test_token_expiry_calculation(self, mock_post, mock_datetime, oauth_manager):
        """Test token expiry calculation."""
        fixed_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = fixed_time

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 7200  # 2 hours
        }
        mock_post.return_value = mock_response

        oauth_manager.get_access_token()

        expected_expiry = fixed_time + timedelta(seconds=7200)
        assert oauth_manager._token_expiry == expected_expiry
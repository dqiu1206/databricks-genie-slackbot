"""
Tests for the config module.
"""

import unittest
from unittest.mock import patch, MagicMock
from ..config import Config, ConfigurationError, GenieError, load_secret, load_configuration


class TestConfig(unittest.TestCase):
    """Test cases for configuration functionality."""
    
    def test_config_constants(self):
        """Test that configuration constants are properly defined."""
        self.assertIsInstance(Config.DATABRICKS_HOST, str)
        self.assertIsInstance(Config.GENIE_SPACE_ID, str)
        self.assertIsInstance(Config.SLACK_BOT_TOKEN, str)
        self.assertIsInstance(Config.SLACK_APP_TOKEN, str)
        self.assertIsInstance(Config.MAX_CONVERSATION_AGE, int)
        self.assertIsInstance(Config.CACHE_TTL, int)
        self.assertIsInstance(Config.CACHE_MAX_SIZE, int)
        self.assertIsInstance(Config.CONNECTION_POOL_SIZE, int)
        self.assertIsInstance(Config.CONNECTION_TIMEOUT, int)
    
    @patch('os.getenv')
    def test_load_secret_with_dbutils(self, mock_getenv):
        """Test loading secrets with dbutils."""
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "test_secret"
        
        result = load_secret("test_scope", "test_key", dbutils=mock_dbutils)
        self.assertEqual(result, "test_secret")
        mock_dbutils.secrets.get.assert_called_once_with("test_scope", "test_key")
    
    @patch('os.getenv')
    def test_load_secret_with_workspace_client(self, mock_getenv):
        """Test loading secrets with workspace client."""
        mock_client = MagicMock()
        mock_client.secrets.get_secret.return_value = "test_secret"
        
        result = load_secret("test_scope", "test_key", workspace_client=mock_client)
        self.assertEqual(result, "test_secret")
        mock_client.secrets.get_secret.assert_called_once_with("test_scope", "test_key")
    
    @patch('os.getenv')
    def test_load_secret_fallback_to_env(self, mock_getenv):
        """Test loading secrets falls back to environment variables."""
        mock_getenv.return_value = "env_secret"
        
        result = load_secret("test_scope", "test_key")
        self.assertEqual(result, "env_secret")
        mock_getenv.assert_called_once_with("test_scope_test_key")


if __name__ == '__main__':
    unittest.main() 
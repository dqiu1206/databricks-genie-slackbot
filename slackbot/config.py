"""
Configuration module for Databricks Genie Slack Bot.

This module handles all configuration loading, validation, and constants.
"""

import os
import logging
from typing import Optional
from pathlib import Path

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    # Look for .env file in current directory and parent directories
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Loaded .env file from {env_path}")
    else:
        # Try current directory
        load_dotenv()
        logger.info("Loaded .env file from current directory")
except ImportError:
    logger.warning("python-dotenv not installed, .env file will not be loaded automatically")


class Config:
    """Centralized Configuration Constants."""
    
    # Core limits
    MAX_PROCESSED_MESSAGES = 1000
    MAX_CONVERSATION_AGE = 3600  # 1 hour
    SLACK_FILE_SIZE_LIMIT = 50 * 1024 * 1024  # 50MB
    
    # Performance settings
    POLL_INTERVAL = 3
    MAX_WAIT_TIME = 120
    CLEANUP_INTERVAL = 300  # 5 minutes
    MEMORY_THRESHOLD = 80  # Cleanup when memory usage > 80%
    
    # Connection pool
    POOL_SIZE = 10
    MAX_POOL_SIZE = 50
    CONNECTION_TIMEOUT = 30
    CONNECTION_MAX_AGE = 3600  # 1 hour
    
    # Query cache
    CACHE_SIZE = 1000
    CACHE_TTL = 1800  # 30 minutes
    
    # Buffer sizes
    BUFFER_SIZE = 1024 * 1024 * 10  # 10MB buffer for large queries
    
    # Thread pool configuration
    MAX_WORKER_THREADS = 20
    WORKER_THREAD_MULTIPLIER = 2  # multiplier for CPU count
    MIN_CPU_COUNT = 4  # fallback CPU count
    
    # Timing intervals
    HEARTBEAT_INTERVAL = 60  # seconds
    SOCKET_CONNECTION_DELAY = 2  # seconds
    MESSAGE_PROCESSING_DELAY = 0.5  # seconds
    PERFORMANCE_UPDATE_INTERVAL = 60  # seconds
    
    # Bot response patterns to prevent loops
    BOT_RESPONSE_PATTERNS = [
        "Genie:", "✅", "❌", "📁", "🔗", "📊", "⏱️", "📋", 
        "💾", "📦", "🆔", "⚠️", "Query generated but could not be executed automatically"
    ]
    
    # System table patterns
    SYSTEM_TABLE_PATTERNS = ['system.billing', 'system.operational_data', 'system.access']


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""
    pass


class GenieError(Exception):
    """Raised when Genie operations fail."""
    pass


def load_secret(scope: str, key: str, dbutils=None, workspace_client=None) -> Optional[str]:
    """Load a secret from Databricks secrets."""
    try:
        if dbutils is not None:
            return dbutils.secrets.get(scope=scope, key=key)
        elif workspace_client is not None:
            secret_response = workspace_client.secrets.get_secret(scope=scope, key=key)
            return secret_response.value if hasattr(secret_response, 'value') else str(secret_response)
    except Exception as e:
        logger.warning(f"Could not load secret {key} from scope {scope}: {e}")
    return None


def load_configuration(bot_state) -> None:
    """Load configuration from environment variables and secrets."""
    # Check for Databricks Apps environment
    app_databricks_host = os.getenv('DATABRICKS_HOST')
    app_client_id = os.getenv('DATABRICKS_CLIENT_ID')
    app_client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
    app_access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    logger.info(f"Databricks Apps environment detected: {app_databricks_host is not None}")
    
    # Check authentication method
    has_oauth2_credentials = app_client_id and app_client_secret
    has_access_token = app_access_token is not None
    
    if not has_oauth2_credentials and not has_access_token:
        raise ConfigurationError(
            "Either DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (OAuth2) "
            "or DATABRICKS_ACCESS_TOKEN (Personal Access Token) is required"
        )
    
    if has_oauth2_credentials and has_access_token:
        logger.warning("Both OAuth2 credentials and Personal Access Token found. Using OAuth2 credentials.")
    
    # Configure connection pool
    bot_state.connection_pool.configure(
        databricks_host=app_databricks_host,
        client_id=app_client_id,
        client_secret=app_client_secret,
        access_token=app_access_token
    )
    
    # Initialize workspace client from pool
    if app_databricks_host:
        bot_state.databricks_host = app_databricks_host
        
        try:
            # Get initial connection to test and configure
            bot_state.workspace_client = bot_state.connection_pool.get_connection()
            auth_method = "OAuth2 service principal" if has_oauth2_credentials else "Personal Access Token"
            logger.info(f"Successfully authenticated using {auth_method}")
            
            # Initialize dbutils
            initialize_dbutils(bot_state.workspace_client, bot_state)
            
            # Return connection to pool for reuse
            bot_state.connection_pool.return_connection(bot_state.workspace_client)
            
        except Exception as e:
            logger.error(f"Failed to initialize workspace client: {e}")
            raise
    
    # Load other configuration
    secret_scope = os.getenv('SECRET_SCOPE', 'slackbot-genie')
    bot_state.genie_space_id = os.getenv('GENIE_SPACE_ID')
    
    # Load Slack tokens
    bot_state.slack_app_token = load_secret(secret_scope, "SLACK_APP_TOKEN", bot_state.dbutils, bot_state.workspace_client) or os.getenv('SLACK_APP_TOKEN')
    bot_state.slack_bot_token = load_secret(secret_scope, "SLACK_BOT_TOKEN", bot_state.dbutils, bot_state.workspace_client) or os.getenv('SLACK_BOT_TOKEN')
    
    # Load SQL query display flag
    show_query_env = os.getenv('SHOW_SQL_QUERY', 'true')
    bot_state.show_sql_query = show_query_env.lower() in ('true', '1', 'yes', 'on')
    
    logger.info(f"Configuration loaded - SHOW_SQL_QUERY: {bot_state.show_sql_query}")


def validate_environment(bot_state) -> None:
    """Validate that all required configuration values are set."""
    required_vars = {
        'DATABRICKS_HOST': bot_state.databricks_host,
        'GENIE_SPACE_ID': bot_state.genie_space_id,
        'SLACK_APP_TOKEN': bot_state.slack_app_token,
        'SLACK_BOT_TOKEN': bot_state.slack_bot_token
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise ConfigurationError(f"Missing required configuration values: {', '.join(missing_vars)}")
    
    logger.info("All required configuration values are set")


def initialize_dbutils(workspace_client, bot_state) -> None:
    """Initialize dbutils with the workspace client for secrets access."""
    try:
        if bot_state.dbutils is None and workspace_client is not None:
            from databricks.sdk.dbutils import RemoteDbUtils
            bot_state.dbutils = RemoteDbUtils(workspace_client._config)
            logger.info("Initialized dbutils with workspace client for secrets access")
    except Exception as e:
        logger.warning(f"Could not initialize dbutils: {e}")
        bot_state.dbutils = None 
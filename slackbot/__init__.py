"""
Databricks Genie Slack Bot Package

A modular Slack bot that integrates with Databricks Genie to provide conversational AI
capabilities for data queries and analysis.

This package maintains full backward compatibility with the original slackbot.py script.
"""

# Import main components for backward compatibility
from .config import Config, ConfigurationError, GenieError
from .databricks_client import DatabricksConnectionPool, get_databricks_client, return_databricks_client
from .slack_handlers import setup_slack_handlers, start_socket_mode, get_slack_app, get_socket_handler
from .routes import app
from .utils import (
    BotState, bot_state, LRUCache, PerformanceMonitor,
    speak_with_genie, process_message_async, handle_special_commands,
    initialize_clients, main, cleanup_and_shutdown
)

# Export all the main functions and classes for backward compatibility
__all__ = [
    # Configuration
    'Config', 'ConfigurationError', 'GenieError',
    
    # Databricks client
    'DatabricksConnectionPool', 'get_databricks_client', 'return_databricks_client',
    
    # Slack handlers
    'setup_slack_handlers', 'start_socket_mode', 'get_slack_app', 'get_socket_handler',
    
    # Flask app
    'app',
    
    # Core functionality
    'BotState', 'bot_state', 'LRUCache', 'PerformanceMonitor',
    'speak_with_genie', 'process_message_async', 'handle_special_commands',
    'initialize_clients', 'main', 'cleanup_and_shutdown'
]

# Version information
__version__ = "2.0.0"
__author__ = "Databricks"
__description__ = "Databricks Genie Slack Bot - Modular Edition" 
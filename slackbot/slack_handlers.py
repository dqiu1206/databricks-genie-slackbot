"""
Slack handlers module for Databricks Genie Slack Bot.

This module handles all Slack-specific logic including event handlers, Socket Mode setup,
and message processing.
"""

import ssl
import time
import logging
import threading
from typing import Dict, Any

import slack_sdk
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .config import Config
from .utils import (
    process_message_async, add_message_to_queue
)

logger = logging.getLogger(__name__)


def get_slack_app(bot_state) -> App:
    """Initialize and return a Slack app with Socket Mode support."""
    try:
        # Create SSL context for Slack
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        # Create Slack client and test connection
        client = slack_sdk.WebClient(token=bot_state.slack_bot_token, ssl=ssl_context)
        auth_test = client.auth_test()
        
        logger.info(f"Connected to Slack workspace: {auth_test['team']}")
        logger.info(f"Bot user: {auth_test['user']}")
        
        # Store bot user ID for loop prevention
        bot_state.bot_user_id = auth_test.get('user_id')
        logger.info(f"Bot user ID: {bot_state.bot_user_id}")
        
        # Create Slack app with enhanced settings
        slack_app = App(
            client=client, 
            process_before_response=True
        )
        logger.info("Successfully initialized Slack app with Socket Mode support")
        
        return slack_app
    except Exception as e:
        logger.error(f"Failed to initialize Slack app: {e}")
        raise


def get_socket_handler(slack_app: App, bot_state) -> SocketModeHandler:
    """Initialize and return a Socket Mode handler."""
    try:
        if not bot_state.slack_app_token:
            raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")
        
        if not bot_state.slack_app_token.startswith('xapp-'):
            raise ValueError("SLACK_APP_TOKEN should start with 'xapp-' (Socket Mode token)")
        
        logger.info(f"Creating Socket Mode handler with app token: {bot_state.slack_app_token[:10]}...")
        handler = SocketModeHandler(slack_app, bot_state.slack_app_token)
        logger.info("Successfully initialized Socket Mode handler")
        return handler
    except Exception as e:
        logger.error(f"Failed to initialize Socket Mode handler: {e}")
        raise


def setup_slack_handlers(bot_state) -> None:
    """Set up Slack event handlers."""
    if bot_state.slack_app is not None:
        logger.info("Setting up Slack event handlers...")
        
        def process_message(event: Dict[str, Any], say, client) -> None:
            """Add message to channel queue for sequential processing."""
            channel_id = event.get("channel", "unknown_channel")
            thread_ts = event.get('thread_ts')
            
            # For threaded messages, process immediately (each thread has its own conversation)
            if thread_ts:
                logger.debug(f"Processing threaded message immediately for channel {channel_id}, thread {thread_ts}")
                bot_state.message_executor.submit(process_message_async, event, say, client, bot_state)
            else:
                # For non-threaded messages, add to queue for sequential processing
                logger.debug(f"Adding non-threaded message to queue for channel {channel_id}")
                add_message_to_queue(channel_id, event, say, client, bot_state)
        
        @bot_state.slack_app.event("message")
        def handle_message(event: Dict[str, Any], say) -> None:
            """Handle direct messages only (not channel messages)."""
            logger.debug(f"📨 Received message event: {event.get('type', 'unknown')}")
            # Only respond to direct messages, not channel messages
            channel_id = event.get("channel", "")
            if channel_id.startswith("D"):  # Direct message channels start with "D"
                logger.debug(f"Processing direct message in channel {channel_id}")
                if bot_state.slack_app and bot_state.slack_app.client:
                    process_message(event, say, bot_state.slack_app.client)
            else:
                logger.debug(f"Ignoring channel message in {channel_id} - only responding to direct messages")

        @bot_state.slack_app.event("app_mention")
        def handle_app_mention(event: Dict[str, Any], say) -> None:
            """Handle app mentions (@botname) in channels."""
            logger.debug(f"📨 Received app_mention event: {event.get('type', 'unknown')}")
            if bot_state.slack_app and bot_state.slack_app.client:
                process_message(event, say, bot_state.slack_app.client)
        
        logger.info("✅ Slack event handlers registered successfully")
    else:
        logger.error("❌ Cannot set up Slack handlers - slack_app is None")


def start_socket_mode(bot_state) -> None:
    """Start the Socket Mode handler in a separate thread."""
    if bot_state.socket_handler is None:
        logger.error("Socket handler not initialized")
        return
    
    def run_socket_handler() -> None:
        try:
            logger.info("Starting Socket Mode handler...")
            if bot_state.socket_handler:
                logger.info("Socket Mode handler is starting - you should see connection messages from slack_bolt")
                bot_state.socket_handler.start()
        except Exception as e:
            logger.error(f"Socket Mode handler error: {e}")
    
    bot_state.socket_thread = threading.Thread(target=run_socket_handler, daemon=True)
    bot_state.socket_thread.start()
    logger.info("Socket Mode handler started in background thread")
    
    # Give it a moment to start and log any immediate errors
    time.sleep(Config.SOCKET_CONNECTION_DELAY)
    if bot_state.socket_thread.is_alive():
        logger.info("✅ Socket Mode thread is running")
    else:
        logger.error("❌ Socket Mode thread failed to start") 
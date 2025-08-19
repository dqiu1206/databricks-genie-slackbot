"""
Slack handlers module for Databricks Genie Slack Bot.

This module handles all Slack-specific logic including event handlers, Socket Mode setup,
and message processing.
"""

import ssl
import time
import logging
import threading
from typing import Dict, Any, Optional

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


def get_channel_info(client, channel_id: str) -> Optional[Dict[str, Any]]:
    """Get channel information to determine channel type and permissions."""
    try:
        # Try to get channel info
        response = client.conversations_info(channel=channel_id)
        if response.get('ok'):
            return response.get('channel', {})
    except Exception as e:
        logger.debug(f"Could not get channel info for {channel_id}: {e}")
    
    return None


def can_bot_post_to_channel(client, channel_id: str) -> bool:
    """Check if the bot can post to a specific channel."""
    # Direct messages (DMs) always allow the bot to post
    if channel_id.startswith("D"):
        logger.debug(f"Channel {channel_id} is a DM - bot can always post")
        return True
    
    try:
        # Get channel info
        channel_info = get_channel_info(client, channel_id)
        if not channel_info:
            return False
        
        # Check if bot is a member of the channel
        is_member = channel_info.get('is_member', False)
        if not is_member:
            logger.debug(f"Bot is not a member of channel {channel_id}")
            return False
        
        # Check channel type
        channel_type = channel_info.get('is_private', False)
        if channel_type:
            logger.debug(f"Channel {channel_id} is private - bot must be explicitly invited")
        else:
            logger.debug(f"Channel {channel_id} is public")
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking bot permissions for channel {channel_id}: {e}")
        return False


def invite_bot_to_channel(client, channel_id: str) -> bool:
    """Attempt to invite the bot to a channel."""
    # Cannot invite bot to DM channels
    if channel_id.startswith("D"):
        logger.debug(f"Cannot invite bot to DM channel {channel_id}")
        return False
    
    try:
        # Get bot user ID
        auth_test = client.auth_test()
        bot_user_id = auth_test.get('user_id')
        
        if not bot_user_id:
            logger.error("Could not get bot user ID")
            return False
        
        # Try to invite bot to channel
        response = client.conversations_invite(
            channel=channel_id,
            users=[bot_user_id]
        )
        
        if response.get('ok'):
            logger.info(f"Successfully invited bot to channel {channel_id}")
            return True
        else:
            error = response.get('error', 'unknown error')
            logger.warning(f"Failed to invite bot to channel {channel_id}: {error}")
            return False
            
    except Exception as e:
        logger.error(f"Error inviting bot to channel {channel_id}: {e}")
        return False


def setup_slack_handlers(bot_state) -> None:
    """Set up Slack event handlers."""
    if bot_state.slack_app is not None:
        logger.info("Setting up Slack event handlers...")
        
        # Add a test handler to verify events are being received
        @bot_state.slack_app.event("hello")
        def handle_hello(event: Dict[str, Any]) -> None:
            """Handle Socket Mode hello event for connection verification."""
            logger.info("👋 Socket Mode connected successfully - received hello event")
        
        def process_message(event: Dict[str, Any], say, client) -> None:
            """Add message to user queue for sequential processing per user."""
            user_id = event.get("user", "unknown_user")
            channel_id = event.get("channel", "unknown_channel")
            thread_ts = event.get('thread_ts')
            
            # All messages are now queued per user regardless of thread status
            # This ensures each user is limited to 1 thread across all conversations
            logger.debug(f"Adding message to queue for user {user_id} in channel {channel_id}")
            add_message_to_queue(user_id, event, say, client, bot_state)
        
        @bot_state.slack_app.event("message")
        def handle_message(event: Dict[str, Any], say) -> None:
            """Handle direct messages only. Channel messages require explicit mentions."""
            logger.info(f"📨 Received message event: {event.get('type', 'unknown')}, channel: {event.get('channel', 'unknown')}, user: {event.get('user', 'unknown')}, text: {event.get('text', '')[:50]}")
            
            # Get channel information
            channel_id = event.get("channel", "")
            user_id = event.get("user", "")
            
            # Skip bot messages to prevent loops
            if event.get('bot_id') or event.get('subtype') in ['bot_message', 'me_message']:
                logger.debug("Ignoring bot message to prevent loops")
                return
            
            # Skip messages from the bot itself
            if bot_state.bot_user_id and user_id == bot_state.bot_user_id:
                logger.debug("Ignoring message from bot itself")
                return
            
            # Only process direct messages (DMs) - channel messages require explicit mentions
            if channel_id.startswith("D"):  # Direct message
                logger.debug(f"Processing direct message in channel {channel_id}")
                if bot_state.slack_app and bot_state.slack_app.client:
                    process_message(event, say, bot_state.slack_app.client)
            else:
                # For channel messages, ignore them - they should be handled by app_mention event
                logger.debug(f"Ignoring channel message in {channel_id} - requires explicit mention (@bot)")
                return

        @bot_state.slack_app.event("app_mention")
        def handle_app_mention(event: Dict[str, Any], say) -> None:
            """Handle app mentions (@botname) in channels and threads."""
            channel_id = event.get("channel", "unknown")
            user_id = event.get("user", "unknown")
            thread_ts = event.get("thread_ts")
            message_text = event.get("text", "")
            
            thread_info = f" in thread {thread_ts}" if thread_ts else ""
            logger.info(f"📨 Received app_mention event: channel: {channel_id}, user: {user_id}{thread_info}")
            logger.info(f"   Mention text: {message_text[:100]}...")
            
            # Skip bot messages to prevent loops
            if event.get('bot_id') or event.get('subtype') in ['bot_message', 'me_message']:
                logger.debug("Ignoring bot mention in bot message to prevent loops")
                return
            
            # Skip messages from the bot itself
            if bot_state.bot_user_id and user_id == bot_state.bot_user_id:
                logger.debug("Ignoring mention from bot itself")
                return
            
            if bot_state.slack_app and bot_state.slack_app.client:
                process_message(event, say, bot_state.slack_app.client)
        
        # Add handler for channel join events to automatically invite bot when needed
        @bot_state.slack_app.event("channel_joined")
        def handle_channel_joined(event: Dict[str, Any]) -> None:
            """Handle when bot joins a channel."""
            channel_id = event.get("channel", {}).get("id")
            channel_name = event.get("channel", {}).get("name", "unknown")
            if channel_id:
                logger.info(f"Bot joined channel #{channel_name} ({channel_id})")
        
        @bot_state.slack_app.event("group_joined")
        def handle_group_joined(event: Dict[str, Any]) -> None:
            """Handle when bot joins a private group."""
            channel_id = event.get("channel", {}).get("id")
            channel_name = event.get("channel", {}).get("name", "unknown")
            if channel_id:
                logger.info(f"Bot joined private group #{channel_name} ({channel_id})")
        
        # Add catch-all handler for debugging
        @bot_state.slack_app.event("*")
        def handle_all_events(event: Dict[str, Any]) -> None:
            """Catch-all handler for debugging unhandled events."""
            event_type = event.get('type', 'unknown')
            if event_type not in ['message', 'app_mention', 'hello', 'channel_joined', 'group_joined']:
                logger.warning(f"🔍 Unhandled event type: {event_type}, event: {event}")
        
        logger.info("✅ Slack event handlers registered successfully")
        logger.info("📡 Registered event handlers: message (DM only), app_mention (channels/threads), channel_joined, group_joined, hello")
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
            bot_state.socket_handler_error = e
    
    bot_state.socket_thread = threading.Thread(target=run_socket_handler, daemon=True)
    bot_state.socket_thread.start()
    logger.info("Socket Mode handler started in background thread")
    
    # Give it a moment to start and verify connection
    time.sleep(Config.SOCKET_CONNECTION_DELAY)
    
    # Check if thread is alive and handler is running
    if bot_state.socket_thread.is_alive():
        logger.info("✅ Socket Mode thread is running")
        
        # Additional verification - check if handler is actually connected
        time.sleep(2)  # Give more time for connection
        if hasattr(bot_state.socket_handler, 'client') and bot_state.socket_handler.client:
            logger.info("✅ Socket Mode WebSocket client is initialized")
        else:
            logger.warning("⚠️ Socket Mode client may not be properly connected")
            
        # Check for any errors during startup
        if hasattr(bot_state, 'socket_handler_error') and bot_state.socket_handler_error:
            logger.error(f"❌ Socket handler encountered error: {bot_state.socket_handler_error}")
    else:
        logger.error("❌ Socket Mode thread failed to start") 
"""
Utility functions and core functionality for Databricks Genie Slack Bot.

This module contains all utility functions, data structures, and core business logic
that doesn't fit into the other specialized modules.
"""

import os
import time
import csv
import io
import hashlib
import logging
import threading
import signal
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    ResourceDoesNotExist, PermissionDenied, BadRequest, 
    InternalError, TooManyRequests
)

from .config import Config, GenieError
from .databricks_client import (
    get_databricks_client, initialize_dbutils, extract_conversation_ids,
    wait_for_message_completion, execute_query_with_fallback, 
    wait_for_query_completion_if_needed
)

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics for monitoring."""
    query_count: int = 0
    avg_response_time: float = 0.0
    memory_usage_mb: float = 0.0
    active_connections: int = 0
    queue_depth: int = 0
    error_count: int = 0
    last_updated: datetime = field(default_factory=datetime.now)


class PerformanceMonitor:
    """Monitor and track performance metrics."""
    
    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.query_times = []
        self.lock = RLock()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._periodic_monitoring, daemon=True)
        self.monitor_thread.start()
    
    def _periodic_monitoring(self):
        """Periodically collect system metrics."""
        while True:
            try:
                time.sleep(Config.PERFORMANCE_UPDATE_INTERVAL)  # Update every minute
                self._update_system_metrics()
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")
    
    def _update_system_metrics(self):
        """Update system-level metrics."""
        try:
            try:
                import psutil
                process = psutil.Process()
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024
            except ImportError:
                logger.debug("psutil not available, setting memory usage to 0")
                memory_usage_mb = 0.0
            
            with self.lock:
                self.metrics.memory_usage_mb = memory_usage_mb
                self.metrics.last_updated = datetime.now()
                
                # Calculate average response time
                if self.query_times:
                    self.metrics.avg_response_time = sum(self.query_times) / len(self.query_times)
                    # Keep only recent times
                    if len(self.query_times) > 100:
                        self.query_times = self.query_times[-50:]
                        
        except Exception as e:
            logger.error(f"Error updating system metrics: {e}")
    
    def record_query(self, response_time: float, success: bool = True):
        """Record a query execution."""
        with self.lock:
            self.metrics.query_count += 1
            if success:
                self.query_times.append(response_time)
            else:
                self.metrics.error_count += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        with self.lock:
            return {
                "query_count": self.metrics.query_count,
                "avg_response_time": self.metrics.avg_response_time,
                "memory_usage_mb": self.metrics.memory_usage_mb,
                "error_count": self.metrics.error_count,
                "last_updated": self.metrics.last_updated.isoformat()
            }


# Global state
class BotState:
    """Centralized state management for the bot with performance optimizations."""
    
    def __init__(self):
        self.workspace_client: Optional[WorkspaceClient] = None
        self.web_client: Optional[Any] = None  # Slack WebClient
        self.socket_mode_client: Optional[Any] = None  # Socket Mode client
        self.socket_thread: Optional[threading.Thread] = None
        self.socket_handler_error: Optional[Exception] = None
        self.bot_user_id: Optional[str] = None
        self.dbutils: Optional[Any] = None
        
        # Configuration
        self.databricks_host: Optional[str] = None
        self.genie_space_id: Optional[str] = None
        self.slack_app_token: Optional[str] = None
        self.slack_bot_token: Optional[str] = None
        self.show_sql_query: bool = True
        
        # Authentication fields for Databricks client
        self.client_id: Optional[str] = None
        self.client_secret: Optional[str] = None
        self.access_token: Optional[str] = None
        
        # Performance optimizations
        self.performance_monitor = PerformanceMonitor()
        
        # Thread-safe data structures
        self.processed_messages = set()
        self.processed_event_ids = set()
        self.conversation_tracker = {}
        self.conversation_timestamps = {}
        
        # User-based message queuing for handling concurrent messages
        self.message_queue = {}  # user_id -> queue of pending messages
        self.queue_locks = {}  # user_id -> lock for queue operations
        self.processing_users = set()  # users currently being processed
        
        # Locks
        self.processed_messages_lock = RLock()
        self.processed_event_ids_lock = RLock()
        self.conversation_lock = RLock()
        self.queue_lock = RLock()
        
        # Enhanced thread pool with dynamic sizing
        initial_workers = min(Config.MAX_WORKER_THREADS, (os.cpu_count() or Config.MIN_CPU_COUNT) * Config.WORKER_THREAD_MULTIPLIER)
        self.message_executor = ThreadPoolExecutor(
            max_workers=initial_workers, 
            thread_name_prefix="slackbot_worker"
        )
        
        # Genie API rate limiting
        self.genie_rate_limiter = threading.Semaphore(Config.GENIE_MAX_CONCURRENT_CONVERSATIONS)
        self.genie_api_calls = []  # Track API call timestamps for rate limiting
        self.genie_calls_lock = RLock()  # Lock for thread-safe API call tracking
        



# Global bot state
bot_state = BotState()





def check_genie_rate_limit(bot_state) -> bool:
    """Check if we can make a Genie API call within rate limits."""
    current_time = time.time()
    
    with bot_state.genie_calls_lock:
        # Remove API calls older than 1 minute
        cutoff_time = current_time - 60
        bot_state.genie_api_calls = [call_time for call_time in bot_state.genie_api_calls if call_time > cutoff_time]
        
        # Check if we're under the rate limit
        if len(bot_state.genie_api_calls) >= Config.GENIE_RATE_LIMIT_PER_MINUTE:
            logger.warning(f"Rate limit reached: {len(bot_state.genie_api_calls)} calls in the last minute")
            return False
        
        # Record this API call
        bot_state.genie_api_calls.append(current_time)
        return True


def wait_for_genie_api_slot(bot_state, timeout: float = 30.0) -> bool:
    """Wait for an available Genie API slot with timeout."""
    try:
        acquired = bot_state.genie_rate_limiter.acquire(timeout=timeout)
        if not acquired:
            logger.warning("Failed to acquire Genie API slot within timeout")
        return acquired
    except Exception as e:
        logger.error(f"Error acquiring Genie API slot: {e}")
        return False


def release_genie_api_slot(bot_state) -> None:
    """Release a Genie API slot."""
    try:
        bot_state.genie_rate_limiter.release()
    except Exception as e:
        logger.error(f"Error releasing Genie API slot: {e}")


def wait_for_message_completion_with_backoff(workspace_client: WorkspaceClient, space_id: str, conversation_id: str, message_id: str):
    """Wait for message completion with exponential backoff after 2 minutes."""
    from .databricks_client import wait_for_message_completion
    
    start_time = time.time()
    poll_interval = Config.GENIE_POLL_INTERVAL
    max_wait_time = Config.GENIE_MESSAGE_TIMEOUT
    backoff_threshold = Config.GENIE_BACKOFF_THRESHOLD
    
    logger.info(f"Waiting for message completion with improved polling (interval: {poll_interval}s, max: {max_wait_time}s)")
    
    while time.time() - start_time < max_wait_time:
        try:
            elapsed = time.time() - start_time
            
            # Use exponential backoff after 2 minutes
            if elapsed > backoff_threshold:
                # Exponential backoff: double the interval, max 30s
                current_interval = min(poll_interval * (2 ** int((elapsed - backoff_threshold) / 30)), 30)
                logger.debug(f"Using exponential backoff: {current_interval}s interval")
            else:
                current_interval = poll_interval
            
            # Try to get message completion
            result = wait_for_message_completion(workspace_client, space_id, conversation_id, message_id)
            if result:  # Message completed
                logger.info(f"Message completed after {elapsed:.1f}s")
                return result
            
            # Wait before next poll
            time.sleep(current_interval)
            
        except Exception as e:
            logger.error(f"Error during message polling: {e}")
            time.sleep(poll_interval)
    
    logger.warning(f"Message polling timed out after {max_wait_time}s")
    return None


def get_conversation_key(channel_id: str, thread_ts: Optional[str], message_ts: Optional[str] = None) -> str:
    """Generate a unique key for tracking conversations by channel and thread."""
    if thread_ts:
        # For threaded conversations, use thread timestamp
        conversation_key = f"{channel_id}:{thread_ts}"
        logger.debug(f"Generated conversation key for thread: {conversation_key}")
        return conversation_key
    else:
        # For non-threaded messages, use message timestamp to create unique conversations
        # This ensures each new message starts a fresh conversation unless it's a reply
        if message_ts:
            conversation_key = f"{channel_id}:{message_ts}"
            logger.debug(f"Generated conversation key for non-threaded message: {conversation_key}")
            return conversation_key
        else:
            # Fallback to channel ID if no message timestamp available
            conversation_key = f"{channel_id}:direct"
            logger.debug(f"Generated fallback conversation key: {conversation_key}")
            return conversation_key


def get_active_conversation_id(channel_id: str, thread_ts: Optional[str], message_ts: Optional[str] = None) -> Optional[str]:
    """Get the active conversation ID for a channel/thread combination if it exists and is not expired."""
    conversation_key = get_conversation_key(channel_id, thread_ts, message_ts)
    current_time = time.time()
    
    logger.debug(f"Looking for active conversation with key: {conversation_key}")
    logger.debug(f"Available conversation keys: {list(bot_state.conversation_tracker.keys())}")
    
    with bot_state.conversation_lock:
        if conversation_key in bot_state.conversation_tracker:
            last_used = bot_state.conversation_timestamps.get(conversation_key, 0)
            age_seconds = current_time - last_used
            logger.debug(f"Found conversation key {conversation_key}, age: {age_seconds:.1f}s, max age: {Config.MAX_CONVERSATION_AGE}s")
            
            if age_seconds < Config.MAX_CONVERSATION_AGE:
                # Update timestamp to keep conversation active
                bot_state.conversation_timestamps[conversation_key] = current_time
                conversation_id = bot_state.conversation_tracker[conversation_key]
                logger.info(f"Found active conversation {conversation_id} for {conversation_key}")
                return conversation_id
            else:
                # Conversation expired, remove it
                logger.info(f"Conversation {bot_state.conversation_tracker[conversation_key]} expired for {conversation_key} (age: {age_seconds:.1f}s)")
                del bot_state.conversation_tracker[conversation_key]
                if conversation_key in bot_state.conversation_timestamps:
                    del bot_state.conversation_timestamps[conversation_key]
        else:
            logger.debug(f"No active conversation found for key: {conversation_key}")
    
    return None


def store_conversation_id(channel_id: str, thread_ts: Optional[str], conversation_id: str, message_ts: Optional[str] = None) -> None:
    """Store a conversation ID for a channel/thread combination."""
    conversation_key = get_conversation_key(channel_id, thread_ts, message_ts)
    with bot_state.conversation_lock:
        bot_state.conversation_tracker[conversation_key] = conversation_id
        bot_state.conversation_timestamps[conversation_key] = time.time()
    logger.info(f"Stored conversation {conversation_id} for {conversation_key}")
    logger.debug(f"Total active conversations: {len(bot_state.conversation_tracker)}")






def get_user_queue_lock(user_id: str, bot_state) -> RLock:
    """Get or create a lock for a specific user's message queue."""
    with bot_state.queue_lock:
        if user_id not in bot_state.queue_locks:
            bot_state.queue_locks[user_id] = RLock()
        return bot_state.queue_locks[user_id]


def add_message_to_queue(user_id: str, event: Dict[str, Any], say, client, bot_state) -> None:
    """Add a message to the user's processing queue."""
    user_lock = get_user_queue_lock(user_id, bot_state)
    
    with user_lock:
        if user_id not in bot_state.message_queue:
            bot_state.message_queue[user_id] = []
        
        # Add message to queue
        bot_state.message_queue[user_id].append((event, say, client))
        queue_length = len(bot_state.message_queue[user_id])
        logger.info(f"📥 Added message to queue for user {user_id}. Queue length: {queue_length}")
        
        # Only show queue message if queue is getting long (more than 3 messages)
        if queue_length > 3:
            try:
                position = queue_length - 1  # 0-based index
                say(f"⏳ *High demand detected* - You are #{position} in line. Processing your request...", thread_ts=event.get("ts"))
            except Exception as e:
                logger.error(f"Error sending queue status message: {e}")
        
        # Start processing if not already processing AND if we have capacity AND user isn't at thread limit
        active_users = len(bot_state.processing_users)
        user_thread_count = 1 if user_id in bot_state.processing_users else 0
        
        if (user_id not in bot_state.processing_users and 
            active_users < Config.MAX_CONCURRENT_USERS and 
            user_thread_count < Config.MAX_THREADS_PER_USER):
            bot_state.processing_users.add(user_id)
            # Submit queue processing to thread pool
            bot_state.message_executor.submit(process_user_queue, user_id, bot_state)


def process_user_queue(user_id: str, bot_state) -> None:
    """Process all messages in a user's queue sequentially."""
    user_lock = get_user_queue_lock(user_id, bot_state)
    
    try:
        while True:
            with user_lock:
                if not bot_state.message_queue.get(user_id):
                    # No more messages in queue
                    break
                
                # Get next message from queue
                event, say, client = bot_state.message_queue[user_id].pop(0)
                logger.info(f"📤 Processing message from queue for user {user_id}. Remaining in queue: {len(bot_state.message_queue[user_id])}")
            
            # Process the message (outside the lock to avoid blocking)
            try:
                process_message_async(event, say, client, bot_state)
                # Small delay between messages to prevent overwhelming the system
                time.sleep(Config.MESSAGE_PROCESSING_DELAY)
            except Exception as e:
                logger.error(f"Error processing queued message for user {user_id}: {e}")
                # Send error response
                try:
                    say(f"❌ Sorry, I encountered an error processing your message: {str(e)}", thread_ts=event.get("ts"))
                except Exception as say_error:
                    logger.error(f"Error sending error message: {say_error}")
    
    finally:
        # Remove user from processing set
        with bot_state.queue_lock:
            bot_state.processing_users.discard(user_id)
        logger.info(f"✅ Finished processing queue for user {user_id}")


def is_bot_message(event: Dict[str, Any], bot_state) -> bool:
    """Check if a message is from the bot itself to prevent loops."""
    # Basic loop prevention - ignore bot messages
    if event.get('bot_id'):
        return True
    
    # Check for bot message subtypes
    if event.get('subtype') in ['bot_message', 'me_message']:
        return True
    
    # Check if the message is from the bot itself
    user_id = event.get("user")
    if user_id and bot_state.bot_user_id and user_id == bot_state.bot_user_id:
        return True
    
    # Fallback check using auth_test if bot_user_id is not set
    if user_id and not bot_state.bot_user_id:
        try:
            # Get the client from bot_state if available
            client = getattr(bot_state, 'slack_app', None)
            if client and hasattr(client, 'client'):
                bot_info = client.client.auth_test()
                if bot_info and bot_info.get('user_id') == user_id:
                    logger.info("Ignoring message from the bot itself to prevent loops (auth_test fallback)")
                    return True
        except Exception as e:
            logger.warning(f"Could not verify bot info: {e}")
    
    return False


def is_duplicate_message(event: Dict[str, Any], message_content: str, bot_state) -> bool:
    """Check if a message has already been processed."""
    user_id = event.get("user", "unknown_user")
    message_ts = event.get("ts", "unknown_ts")
    event_id = event.get("event_id", "no_event_id")
    
    # Create message hash from content
    message_hash = hashlib.md5(message_content.encode()).hexdigest()[:8]
    message_key = (user_id, message_ts, message_hash)
    
    # Check if this message was already processed
    with bot_state.processed_messages_lock:
        if message_key in bot_state.processed_messages:
            logger.debug(f"Ignoring duplicate message: {message_key}")
            return True
    
    # Check if this event ID was already processed
    with bot_state.processed_event_ids_lock:
        if event_id != "no_event_id" and event_id in bot_state.processed_event_ids:
            logger.debug(f"Ignoring duplicate event ID: {event_id}")
            return True
    
    return False


def is_bot_response_pattern(message_content: str) -> bool:
    """Check if message starts with bot response patterns to prevent loops."""
    message_content_stripped = message_content.strip()
    return any(message_content_stripped.startswith(pattern) for pattern in Config.BOT_RESPONSE_PATTERNS)


def handle_special_commands(message_content: str, channel_id: str, thread_ts: Optional[str], message_ts: Optional[str], say, bot_state) -> bool:
    """Handle special bot commands. Returns True if command was handled."""
    message_content_stripped = message_content.strip().lower()
    
    # Handle conversation reset command
    if message_content_stripped in ['/reset', 'reset', 'new conversation', 'start over', 'clear conversation']:
        conversation_key = get_conversation_key(channel_id, thread_ts, message_ts)
        try:
            with bot_state.conversation_lock:
                if conversation_key in bot_state.conversation_tracker:
                    old_conversation_id = bot_state.conversation_tracker[conversation_key]
                    del bot_state.conversation_tracker[conversation_key]
                    if conversation_key in bot_state.conversation_timestamps:
                        del bot_state.conversation_timestamps[conversation_key]
                    logger.info(f"Reset conversation for {conversation_key} (was: {old_conversation_id})")
                    say("🔄 *Conversation history cleared!* Your next message will start a fresh conversation with Genie.", thread_ts=message_ts)
                else:
                    say("🔄 *No active conversation to clear.* Your next message will start a fresh conversation with Genie.", thread_ts=message_ts)
        except Exception as e:
            logger.error(f"Error resetting conversation: {e}")
            say("🔄 *Conversation reset attempted.* Your next message will start a fresh conversation with Genie.", thread_ts=message_ts)
        return True
    
    # Handle help command
    if message_content_stripped in ['/help', 'help', 'commands']:
        help_text = [
            "🤖 *Genie Slackbot Commands:*",
            "",
            "💬 *Chat with Genie:* Just send any message to start a conversation!",
            "🔄 *Reset conversation:* Type `/reset` to start a fresh conversation",
            "📊 *Conversation status:* Type `/status` to see your current conversation",
            "❓ *Get help:* Type `/help` to see this message",
            "",
            "💡 *Features:*",
            "• **Follow-up questions:** Reply in the same Slack thread to continue conversations",
            "• **Context retention:** Genie remembers previous messages in threads",
            "• **Thread-based conversations:** Each Slack thread maintains its own conversation",
            "• **Automatic expiration:** Conversations expire after 1 hour of inactivity",
            "",
            "📊 *Query Results:*",
            "• Genie can generate and execute SQL queries",
            "• Results are provided as CSV files when available",
            "• Large files may be too big for Slack upload",
            "",
            "Need help? Contact your workspace administrator."
        ]
        say("\n".join(help_text), thread_ts=message_ts)
        return True
    
    # Handle conversation status command
    if message_content_stripped in ['/status', 'status']:
        conversation_key = get_conversation_key(channel_id, thread_ts, message_ts)
        try:
            with bot_state.conversation_lock:
                if conversation_key in bot_state.conversation_tracker:
                    conversation_id = bot_state.conversation_tracker[conversation_key]
                    last_used = bot_state.conversation_timestamps.get(conversation_key, 0)
                    age_minutes = (time.time() - last_used) / 60
                    status_text = [
                        "📊 *Conversation Status:*",
                        f"• **Active conversation ID:** `{conversation_id}`",
                        f"• **Thread:** {thread_ts or message_ts}",
                        f"• **Age:** {age_minutes:.1f} minutes",
                        "",
                        "💡 *Tip:* Reply in this thread to continue the conversation",
                        "🔄 *Tip:* Use `/reset` to clear conversation history and start fresh"
                    ]
                else:
                    status_text = [
                        "📊 *Conversation Status:*",
                        "• **No active conversation**",
                        "",
                        "💡 *Tip:* Send any message to start a conversation with Genie!"
                    ]
        except Exception as e:
            logger.error(f"Error getting conversation status: {e}")
            status_text = [
                "📊 *Conversation Status:*",
                "• **Error retrieving status**",
                "",
                "💡 *Tip:* Send any message to start a conversation with Genie!"
            ]
        
        say("\n".join(status_text), thread_ts=message_ts)
        return True
    
    return False 


def extract_message_content(message_result: Any) -> Tuple[List[str], Optional[Any], Optional[str]]:
    """Extract text content and query attachment from message result."""
    genie_text_response = []
    genie_query_attachment = None
    attachment_id = None
    
    # Try to get direct content first
    try:
        content = getattr(message_result, 'content', None)
        if content:
            logger.debug(f"Message content: {content[:200]}...")
            genie_text_response.append(content)
    except (KeyError, AttributeError):
        pass
    
    # Process attachments
    try:
        attachments = getattr(message_result, 'attachments', None)
        if attachments:
            logger.debug(f"Found {len(attachments)} attachments")
            for attachment in attachments:
                # Handle text attachments
                try:
                    text_attr = getattr(attachment, 'text', None)
                    if text_attr:
                        try:
                            content = getattr(text_attr, 'content', None)
                            if content:
                                genie_text_response.append(content)
                                logger.debug(f"Found text attachment: {content[:100]}...")
                        except (KeyError, AttributeError):
                            if hasattr(text_attr, '__str__'):
                                genie_text_response.append(str(text_attr))
                                logger.debug(f"Found text attachment (direct): {str(text_attr)[:100]}...")
                except (KeyError, AttributeError):
                    pass
                
                # Handle query attachments
                try:
                    query_attr = getattr(attachment, 'query', None)
                    if query_attr:
                        genie_query_attachment = query_attr
                        # Extract attachment ID
                        attachment_id = getattr(attachment, 'attachment_id', None)
                        if not attachment_id:
                            attachment_id = getattr(attachment, 'id', None)
                        if not attachment_id:
                            attachment_id = getattr(query_attr, 'id', None)
                        if not attachment_id:
                            attachment_id = getattr(query_attr, 'attachment_id', None)
                        if not attachment_id:
                            try:
                                metadata = getattr(attachment, 'metadata', None)
                                if metadata:
                                    attachment_id = getattr(metadata, 'id', None)
                            except (KeyError, AttributeError):
                                pass
                        logger.debug(f"Found query attachment: {query_attr.description}")
                        logger.debug(f"Attachment ID: {attachment_id}")
                except (KeyError, AttributeError):
                    pass
                
                # Try alternative attachment types
                try:
                    if hasattr(attachment, 'content') and not genie_text_response:
                        content = getattr(attachment, 'content', None)
                        if content:
                            genie_text_response.append(content)
                            logger.debug(f"Found direct content attachment: {content[:100]}...")
                except (KeyError, AttributeError):
                    pass
    except (KeyError, AttributeError) as e:
        logger.error(f"Error accessing attachments: {e}")
    
    # Fallback: try alternative content fields
    if not genie_text_response and not genie_query_attachment:
        logger.debug("No text response or query attachment found in message result")
        try:
            for field in ['text', 'body', 'message', 'response', 'answer']:
                try:
                    field_value = getattr(message_result, field, None)
                    if field_value:
                        logger.debug(f"Found content in field '{field}': {field_value[:100]}...")
                        genie_text_response.append(str(field_value))
                        break
                except (KeyError, AttributeError):
                    continue
        except Exception as e:
            logger.error(f"Error checking alternative content fields: {e}")
    
    return genie_text_response, genie_query_attachment, attachment_id


def format_query_summary(query_result: Any) -> str:
    """Create a summary of the query execution."""
    try:
        summary_parts = []
        
        try:
            statement_response = getattr(query_result, 'statement_response', None)
            if statement_response:
                # Get execution time
                try:
                    status = getattr(statement_response, 'status', None)
                    if status:
                        try:
                            execution_time = getattr(status, 'execution_time', None)
                            if execution_time:
                                execution_time_sec = execution_time / 1000.0
                                summary_parts.append(f"⏱️ *Execution time:* {execution_time_sec:.2f} seconds")
                        except (KeyError, AttributeError):
                            pass
                except (KeyError, AttributeError):
                    pass
                
                # Get row count from manifest
                try:
                    manifest = getattr(statement_response, 'manifest', None)
                    if manifest:
                        try:
                            total_row_count = getattr(manifest, 'total_row_count', None)
                            if total_row_count:
                                summary_parts.append(f"📈 *Rows returned:* {total_row_count}")
                        except (KeyError, AttributeError):
                            pass
                except (KeyError, AttributeError):
                    pass
        except (KeyError, AttributeError):
            pass
        
        return " | ".join(summary_parts) if summary_parts else "✅ Query completed successfully"
        
    except Exception as e:
        logger.error(f"Error creating query summary: {e}")
        return "✅ Query completed successfully"


def speak_with_genie(msg_input: str, workspace_client: WorkspaceClient, conversation_id: Optional[str] = None, bot_state=None) -> Tuple[str, Optional[bytes], Optional[str], Optional[str]]:
    """Send a message to Databricks Genie and get a response with API best practices."""
    start_time = time.time()
    api_slot_acquired = False
    
    try:
        logger.info(f"Processing query: {msg_input[:50]}...")
        
        # Wait for API slot and check rate limits
        if not wait_for_genie_api_slot(bot_state):
            return "⏳ System is at capacity. Please try again in a moment.", None, None, None
        
        api_slot_acquired = True
        
        if not check_genie_rate_limit(bot_state):
            return "⏳ Rate limit reached. Please wait a moment before trying again.", None, None, None
        
        # Start or continue conversation
        if conversation_id:
            logger.info(f"Continuing conversation {conversation_id}")
            conv_response = workspace_client.genie.create_message(
                space_id=str(bot_state.genie_space_id),
                conversation_id=conversation_id,
                content=msg_input
            )
            
            # Check if response is valid
            if not conv_response:
                logger.error(f"Received None response from create_message for conversation {conversation_id}")
                return "Error: Failed to create message in conversation.", None, None, conversation_id
            
            # Use safe extraction for message ID in continued conversations
            from .databricks_client import safe_getattr, safe_extract_id
            message = safe_getattr(conv_response, 'message')
            message_id = safe_getattr(message, 'id') if message else None
            if not message_id:
                message_id = safe_extract_id(conv_response, ['id', 'message_id'])
            
            # Log the extracted message ID for debugging
            logger.info(f"Extracted message_id: {message_id} for conversation: {conversation_id}")
            conv_id = conversation_id
        else:
            logger.info("Starting new conversation")
            conv_response = workspace_client.genie.start_conversation(
                space_id=str(bot_state.genie_space_id), 
                content=msg_input
            )
            # Extract conversation and message IDs
            conv_id, message_id = extract_conversation_ids(conv_response)
        
        if not conv_id or not message_id:
            logger.error(f"Missing conversation_id ({conv_id}) or message_id ({message_id}) in response")
            if conversation_id:
                # This was a continued conversation, so we know the conv_id
                return f"Error: Could not retrieve message ID from Genie's response for conversation {conv_id}.", None, None, conv_id
            else:
                # This was a new conversation
                return "Error: Could not retrieve conversation details from Genie's response.", None, None, conv_id

        # Wait for message completion with improved polling (5-10s intervals)
        message_result = wait_for_message_completion_with_backoff(
            workspace_client, str(bot_state.genie_space_id), conv_id, message_id
        )
        
        if not message_result:
            return "Error: Failed to get message response from Genie.", None, None, conv_id

        # Extract and process content
        genie_text_response, genie_query_attachment, attachment_id = extract_message_content(message_result)
        
        if genie_query_attachment:
            # Process query attachment
            result = process_query_attachment(
                workspace_client, conv_id, message_id, attachment_id, 
                genie_query_attachment, genie_text_response, bot_state
            )
        else:
            # Return text-only response
            response_text = ' '.join(genie_text_response) if genie_text_response else 'No response from Genie.'
            
            # Clean up response - remove echoed input if it appears at the beginning
            if response_text.lower().startswith(msg_input.lower().strip()):
                # Remove the echoed input and any following whitespace
                cleaned_response = response_text[len(msg_input):].strip()
                if cleaned_response:
                    response_text = cleaned_response
            
            result = f"Genie: {response_text}", None, None, conv_id
        
        response_time = time.time() - start_time
        if bot_state and hasattr(bot_state, 'performance_monitor'):
            bot_state.performance_monitor.record_query(response_time, success=True)
        return result
        
    except Exception as e:
        logger.error(f"Error in Genie communication: {e}")
        response_time = time.time() - start_time
        if bot_state and hasattr(bot_state, 'performance_monitor'):
            bot_state.performance_monitor.record_query(response_time, success=False)
        return create_error_response(e, "Genie communication")
    
    finally:
        # Always release the API slot
        if api_slot_acquired:
            release_genie_api_slot(bot_state)


def process_query_attachment(workspace_client: WorkspaceClient, conv_id: str, message_id: str, 
                            attachment_id: Optional[str], query_attachment: Any, 
                            genie_text_response: List[str], bot_state) -> Tuple[str, Optional[bytes], Optional[str], Optional[str]]:
    """Process query attachment with simplified execution."""
    try:
        logger.info(f"Processing query attachment")
        
        # Execute query with fallback strategies
        query_result = execute_query_with_fallback(
            workspace_client, str(bot_state.genie_space_id), conv_id, message_id, 
            attachment_id, query_attachment
        )
        
        response_parts = []
        response_parts.append(f"Genie generated a query: {query_attachment.description}")
        
        # Only show the SQL query if SHOW_SQL_QUERY is enabled
        if bot_state.show_sql_query:
            response_parts.append(f"Query: ```sql\n{query_attachment.query}\n```")
        
        # Handle query result if available
        if query_result:
            # Check if query result needs to wait for completion
            query_result = wait_for_query_completion_if_needed(query_result, workspace_client)
            
            # Handle query result with proper formatting
            response_parts.append(format_query_summary(query_result))
            
            # Create CSV file from query results
            csv_bytes, filename, _ = create_csv_from_query_results(query_result, workspace_client, query_attachment.query)
            
            # Add preview of results to text response
            preview_text = format_query_results(query_result, workspace_client)
            response_parts.append(preview_text)
        else:
            # No query results available
            csv_bytes, filename = None, None
            
            # Check if this is a system table query and provide specific guidance
            query_text = query_attachment.query.lower()
            is_system_query = any(table in query_text for table in Config.SYSTEM_TABLE_PATTERNS)
            
            if is_system_query:
                response_parts.extend([
                    "⚠️ *Query generated but could not be executed automatically.*",
                    "This query accesses system tables which may require special permissions or workspace configuration.",
                    "💡 **Suggestions:**",
                    "• Check that your workspace has billing data enabled",
                    "• Verify you have the necessary permissions to access system tables",
                    "• Try running the query manually in your Databricks workspace",
                    "• Contact your workspace administrator if you need access to billing data",
                    "• The Genie space may need to be configured with appropriate warehouse permissions"
                ])
            else:
                response_parts.extend([
                    "⚠️ *Query generated but could not be executed automatically.*",
                    "You can copy the SQL query above and run it manually in your Databricks workspace.",
                    "💡 **Common reasons for failure:**",
                    "• Genie space warehouse configuration issues",
                    "• No available warehouses",
                    "• Insufficient permissions",
                    "• Network connectivity issues",
                    "• Query timeout",
                    "• Genie space may need to be reconfigured with proper warehouse access"
                ])
        
        # Add download information for large files
        if csv_bytes:
            file_size_bytes = len(csv_bytes)
            if file_size_bytes >= 1024 * 1024:  # 1MB or larger
                file_size_mb = file_size_bytes / (1024 * 1024)
                size_str = f"{file_size_mb:.1f}MB"
            elif file_size_bytes >= 1024:  # 1KB or larger
                file_size_kb = file_size_bytes / 1024
                size_str = f"{file_size_kb:.1f}KB"
            else:
                size_str = f"{file_size_bytes}B"
            
            if file_size_bytes > Config.SLACK_FILE_SIZE_LIMIT:
                response_parts.append(f"📁 *File too large for Slack upload ({size_str})*")
        
        return "\n\n".join(response_parts), csv_bytes, filename, conv_id
        
    except Exception as e:
        logger.error(f"Error processing query attachment: {e}")
        # Fallback to text-only response if query results fail
        response_parts = []
        response_parts.append(f"Genie generated a query: {query_attachment.description}")
        
        # Only show the SQL query if SHOW_SQL_QUERY is enabled
        if bot_state.show_sql_query:
            response_parts.append(f"Query: ```sql\n{query_attachment.query}\n```")
            
        response_parts.append(f"⚠️ Could not retrieve query results: {str(e)}")
        
        return "\n\n".join(response_parts), None, None, conv_id


def extract_query_data(query_result: Any) -> Tuple[Optional[Any], Optional[Any], Optional[Any]]:
    """Extract statement_response, manifest, and result_data from query result."""
    statement_response = None
    manifest = None
    result_data = None
    
    # Try to get statement_response (for statement execution results)
    statement_response = getattr(query_result, 'statement_response', None)
    if statement_response:
        manifest = getattr(statement_response, 'manifest', None)
        result_data = getattr(statement_response, 'result', None)
    
    # Try Genie query result structure if no statement_response
    if not statement_response:
        for attr in ['result', 'data', 'rows']:
            if hasattr(query_result, attr):
                result_data = getattr(query_result, attr, None)
                break
        
        # Try to get manifest from query_result directly
        manifest = getattr(query_result, 'manifest', None)
    
    # Final attempt to get result_data from statement_response
    if not result_data and statement_response:
        result_data = getattr(statement_response, 'result', None)
    
    return statement_response, manifest, result_data


def extract_column_names(manifest: Any, data_array: Any) -> List[str]:
    """Extract column names from manifest or infer from data."""
    column_names = []
    
    # Try to get column names from schema
    if manifest:
        schema = getattr(manifest, 'schema', None)
        if schema:
            columns = getattr(schema, 'columns', None)
            if columns:
                column_names = [col.name for col in columns]
    
    # If no column names from schema, try to extract from data
    if not column_names and data_array and len(data_array) > 0:
        first_row = data_array[0]
        if hasattr(first_row, '__dict__'):
            column_names = list(first_row.__dict__.keys())
        elif isinstance(first_row, dict):
            column_names = list(first_row.keys())
        elif isinstance(first_row, (list, tuple)):
            column_names = [f"column_{i+1}" for i in range(len(first_row))]
    
    # Final fallback: generate column names based on data structure
    if not column_names and data_array:
        max_cols = 1
        for row in data_array:
            if isinstance(row, (list, tuple)):
                max_cols = max(max_cols, len(row))
            elif isinstance(row, dict):
                max_cols = max(max_cols, len(row))
        column_names = [f"column_{i+1}" for i in range(max_cols)]
    
    return column_names


def extract_data_array(result_data: Any) -> List[Any]:
    """Extract data array from result_data."""
    if not result_data:
        return []
    
    # Try to get data_array directly
    data_array = getattr(result_data, 'data_array', None)
    if data_array:
        return list(data_array)
    
    # Try alternative data extraction methods
    for attr_name in ['data', 'rows', 'values', 'results', 'content']:
        attr_value = getattr(result_data, attr_name, None)
        if attr_value and isinstance(attr_value, (list, tuple)):
            return list(attr_value)
    
    return []


def format_query_results(query_result: Any, workspace_client: Optional[WorkspaceClient] = None) -> str:
    """Format query results in a readable way for Slack."""
    try:
        # Extract data using utility functions
        statement_response, manifest, result_data = extract_query_data(query_result)
        
        if not result_data:
            return "Query executed successfully but no data returned."
        
        # Check for external links (large result sets)
        external_links = getattr(result_data, 'external_links', None)
        if external_links:
            summary_parts = ["✅ **Query executed successfully**"]
            
            # Add metadata
            if manifest:
                total_row_count = getattr(manifest, 'total_row_count', None)
                if total_row_count:
                    summary_parts.append(f"📊 *Total rows:* {total_row_count}")
                
                total_byte_count = getattr(manifest, 'total_byte_count', None)
                if total_byte_count:
                    summary_parts.append(f"💾 *Data size:* {total_byte_count} bytes")
            
            column_names = extract_column_names(manifest, None)
            if column_names:
                summary_parts.append(f"📋 *Columns:* {', '.join(column_names)}")
            
            summary_parts.append("\n*Note: Full results are available for download. Check the message below for the CSV file.*")
            return "\n".join(summary_parts)
        
        # Get data array and process smaller result sets
        data_array = extract_data_array(result_data)
        column_names = extract_column_names(manifest, data_array)
        
        if not data_array:
            # Return summary with available metadata
            summary_parts = ["✅ **Query executed successfully**"]
            
            # Add execution metadata
            if statement_response:
                statement_id = getattr(statement_response, 'statement_id', None)
                if statement_id:
                    summary_parts.append(f"🆔 *Statement ID:* `{statement_id}`")
                
                status = getattr(statement_response, 'status', None)
                if status:
                    execution_time = getattr(status, 'execution_time', None)
                    if execution_time:
                        execution_time_sec = execution_time / 1000.0
                        summary_parts.append(f"⏱️ *Execution time:* {execution_time_sec:.2f} seconds")
            
            if column_names:
                summary_parts.append(f"📋 *Columns:* {', '.join(column_names)}")
            
            return "\n".join(summary_parts)
        
        # Format results summary
        formatted_parts = ["📊 **Query Results Summary:**"]
        formatted_parts.append(f"*{len(data_array)} row(s) returned*")
        
        if column_names:
            formatted_parts.append(f"📋 *Columns:* {', '.join(column_names)}")
        
        return "\n".join(formatted_parts)
        
    except Exception as e:
        logger.error(f"Error formatting query results: {e}")
        return f"Query executed successfully but encountered an error formatting results: {str(e)}"


def create_csv_from_query_results(query_result: Any, workspace_client: WorkspaceClient, query_text: Optional[str] = None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    """Create a CSV file from query results."""
    try:
        logger.debug(f"Creating CSV from query result of type: {type(query_result)}")
        
        # Extract data using utility functions
        statement_response, manifest, result_data = extract_query_data(query_result)
        
        if not result_data:
            logger.debug("No result data found for CSV creation")
            return None, None, None
        
        # Get data array and column names
        data_array = extract_data_array(result_data)
        if not data_array:
            return None, None, None
        
        column_names = extract_column_names(manifest, data_array)
        
        # Create CSV content
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write header if we have column names
        if column_names:
            csv_writer.writerow(column_names)
        
        # Write data rows
        for row in data_array:
            row_data = convert_row_to_list(row, column_names)
            
            # Clean and escape data for CSV
            cleaned_row = []
            for cell in row_data:
                if cell is None:
                    cleaned_row.append("")
                else:
                    # Convert to string and handle special characters
                    cell_str = str(cell).replace('\r', ' ').replace('\n', ' ')
                    cleaned_row.append(cell_str)
            
            csv_writer.writerow(cleaned_row)
        
        csv_content = csv_buffer.getvalue()
        csv_bytes = csv_content.encode('utf-8')
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"query_results_{timestamp}.csv"
        
        logger.info(f"CSV created successfully: {filename} ({len(csv_bytes)} bytes, {len(data_array)} rows)")
        return csv_bytes, filename, None
        
    except Exception as e:
        logger.error(f"Error creating CSV from query results: {e}")
        return None, None, None


def convert_row_to_list(row: Any, column_names: List[str]) -> List[str]:
    """Convert a row of data to a list format for CSV writing."""
    if isinstance(row, (list, tuple)):
        row_data = list(row)
    elif isinstance(row, dict):
        # For dictionary rows, extract values in the order of column names
        if column_names:
            row_data = [row.get(col, "") for col in column_names]
        else:
            row_data = list(row.values())
    elif hasattr(row, '__dict__'):
        # For object rows, extract values in the order of column names
        if column_names:
            row_data = [getattr(row, col, "") for col in column_names]
        else:
            row_data = list(row.__dict__.values())
    else:
        # For single values, wrap in a list
        row_data = [str(row)]
    
    # Pad or truncate to match column count
    if column_names:
        while len(row_data) < len(column_names):
            row_data.append("")
        row_data = row_data[:len(column_names)]
    
    return row_data


def process_message_async(event: Dict[str, Any], say, client, bot_state) -> None:
    """Process incoming Slack messages asynchronously in thread pool."""
    try:
        # Log event details for debugging
        event_type = event.get("type", "unknown_type")
        event_ts = event.get("ts", "no_ts")
        event_id = event.get("event_id", "no_event_id")
        logger.info(f"🔍 Processing event - Type: {event_type}, TS: {event_ts}, Event ID: {event_id}")
        
        # Check if this is a bot message
        if is_bot_message(event, bot_state):
            logger.info("Ignoring bot message to prevent loops")
            return
        
        # Get message content
        message_content = event.get("text", "")
        if not message_content:
            logger.info("Received empty message, ignoring")
            return
        
        # Check for duplicates
        if is_duplicate_message(event, message_content, bot_state):
            logger.info("Ignoring duplicate message")
            return
        
        # Check for bot response patterns
        if is_bot_response_pattern(message_content):
            logger.info("Ignoring message that starts with bot response pattern to prevent loops")
            return
        
        # Extract channel information
        channel_id = event.get("channel", "unknown_channel")
        thread_ts = event.get('thread_ts')
        message_ts = event.get("ts")
        user_id = event.get("user", "unknown_user")
        
        # Handle special commands
        if handle_special_commands(message_content, channel_id, thread_ts, message_ts, say, bot_state):
            logger.info("Handled special command")
            return
        
        # Log message information
        logger.info(f"📨 Message received from user {user_id} in channel {channel_id}")
        logger.info(f"   Event type: {event_type}")
        logger.info(f"   Timestamp: {message_ts}")
        logger.info(f"   Thread timestamp: {thread_ts}")
        logger.info(f"   Message content: {message_content[:100]}{'...' if len(message_content) > 100 else ''}")



        # Get workspace client from bot state
        workspace_client = bot_state.workspace_client
        if workspace_client is None:
            logger.error("Databricks client not initialized")
            # Use proper threading: thread_ts if in thread, message_ts if not
            error_thread_ts = thread_ts if thread_ts else message_ts
            say("❌ Databricks client not initialized. Please check the service configuration.", thread_ts=error_thread_ts)
            return
        
        # Determine the correct thread_ts for replies:
        # - If message is already in a thread, use the existing thread_ts to stay in that thread
        # - If message is not in a thread, use message_ts to create a new thread
        reply_thread_ts = thread_ts if thread_ts else message_ts
        
        try:
            # Check for existing conversation in this thread
            existing_conversation_id = get_active_conversation_id(channel_id, thread_ts, message_ts)
            
            if existing_conversation_id:
                logger.info(f"🔄 Continuing existing conversation {existing_conversation_id} in thread {thread_ts or 'new'} for user {user_id} in channel {channel_id}")
            else:
                logger.info(f"🆕 Starting new conversation in thread {thread_ts or 'new'} for user {user_id} in channel {channel_id}")
            

            
            # Get response from Genie (continue existing or start new) - now using optimized SDK version
            try:
                logger.info(f"🤖 Calling optimized speak_with_genie with message: {message_content[:50]}...")
                genie_response, csv_bytes, filename, conversation_id = speak_with_genie_optimized(
                    message_content, 
                    workspace_client, 
                    existing_conversation_id,  # Use existing conversation if available
                    bot_state
                )
                logger.info(f"✅ Received response from Genie, length: {len(genie_response) if genie_response else 0}")
            except Exception as genie_error:
                logger.error(f"Error in speak_with_genie: {genie_error}")
                genie_response = f"❌ Sorry, I encountered an error while processing your request: {str(genie_error)}"
                csv_bytes, filename, conversation_id = None, None, None
        
        except Exception as e:
            logger.error(f"Error in conversation processing: {e}")
            genie_response = f"❌ Sorry, I encountered an error while processing your request: {str(e)}"
            csv_bytes, filename, conversation_id = None, None, None
        
        # Store the conversation ID if we got one (for new conversations or to update existing)
        if conversation_id:
            store_conversation_id(channel_id, thread_ts, conversation_id, message_ts)
            if existing_conversation_id:
                logger.info(f"✅ Continued conversation {conversation_id}")
            else:
                logger.info(f"✅ Started new conversation {conversation_id}")

        # Send text response back to Slack
        logger.info(f"📤 Sending response to user {user_id} in channel {channel_id}")
        logger.info(f"   Response length: {len(genie_response)} characters")
        logger.info(f"   Response preview: {genie_response[:100]}{'...' if len(genie_response) > 100 else ''}")
        logger.info(f"   Reply thread_ts: {reply_thread_ts} (original thread_ts: {thread_ts}, message_ts: {message_ts})")
        
        say(genie_response, thread_ts=reply_thread_ts)
        logger.info(f"✅ Response sent successfully to user {user_id}")

        # Upload CSV file if available and within size limits
        if csv_bytes and filename:
            file_size_bytes = len(csv_bytes)
            if file_size_bytes >= 1024 * 1024:  # 1MB or larger
                file_size_mb = file_size_bytes / (1024 * 1024)
                size_str = f"{file_size_mb:.1f}MB"
                logger.info(f"Generated CSV file: {filename}, size: {file_size_mb:.2f}MB")
            elif file_size_bytes >= 1024:  # 1KB or larger
                file_size_kb = file_size_bytes / 1024
                size_str = f"{file_size_kb:.1f}KB"
                logger.info(f"Generated CSV file: {filename}, size: {file_size_kb:.2f}KB")
            else:
                size_str = f"{file_size_bytes}B"
                logger.info(f"Generated CSV file: {filename}, size: {file_size_bytes}B")
            
            if file_size_bytes <= Config.SLACK_FILE_SIZE_LIMIT:
                try:
                    logger.info(f"📁 Uploading CSV file to user {user_id}: {filename} ({size_str})")
                    
                    # Upload file to Slack - use proper thread timestamp
                    response = client.files_upload_v2(
                        channel=event.get("channel"),
                        thread_ts=reply_thread_ts,
                        file=csv_bytes,
                        filename=filename,
                        title=f"Query Results - {filename}",
                        initial_comment="📁 Here's your query results file:"
                    )
                    logger.info(f"✅ Successfully uploaded file {filename} to user {user_id}")
                    
                    # Add a confirmation message
                    say(f"✅ *File uploaded successfully!* {filename} ({size_str})", thread_ts=reply_thread_ts)
                    
                except Exception as upload_error:
                    logger.error(f"Error uploading file to Slack: {upload_error}")
                    # If upload fails, provide error message
                    say(f"📁 *File upload failed: {str(upload_error)}*", thread_ts=reply_thread_ts)
            else:
                # File too large, provide guidance
                say(f"📁 *File too large for Slack ({size_str}). Please try a more specific query with LIMIT clause.*", thread_ts=reply_thread_ts)
        
        # Mark this message as processed
        message_hash = hashlib.md5(message_content.encode()).hexdigest()[:8]
        message_key = (user_id, message_ts, message_hash)
        
        with bot_state.processed_messages_lock:
            bot_state.processed_messages.add(message_key)
        
        # Mark event ID as processed
        with bot_state.processed_event_ids_lock:
            if event_id != "no_event_id":
                bot_state.processed_event_ids.add(event_id)
        
        logger.info(f"✅ Marked message as processed - Message key: {message_key}, Event ID: {event_id}")
        logger.info(f"✅ Completed processing for user {user_id}")

    except Exception as e:
        logger.error(f"❌ Error handling message from user {event.get('user', 'unknown')}: {e}")
        error_message = str(e)
        
        # Provide more user-friendly error messages
        if "InterruptedIOException" in error_message:
            suggestions = [
                "❌ The query was interrupted due to a network timeout.",
                "",
                "💡 **Suggestions to fix this:**",
                "• Try adding a `LIMIT` clause to your query (e.g., `LIMIT 100`)",
                "• Simplify the query by selecting fewer columns",
                "• Add a `WHERE` clause to filter the data",
                "• Check your network connection",
                "• Try the query again in a few moments"
            ]
            # Always use message timestamp for error messages to create threads
            say("\n".join(suggestions), thread_ts=event.get("ts"))
        elif "BAD_REQUEST" in error_message:
            say("❌ The query failed due to a bad request. This might be due to invalid SQL syntax or unsupported operations. Please check your query and try again.", thread_ts=event.get("ts"))
        elif "Query execution failed" in error_message:
            say(f"❌ Query execution failed: {error_message}", thread_ts=event.get("ts"))
        else:
            say(f"❌ Sorry, I encountered an error: {error_message}", thread_ts=event.get("ts"))
        
        logger.info(f"✅ Cleaned up processing state for user {event.get('user', 'unknown')} after error")


def handle_genie_error(error: Exception, operation: str = "Genie operation") -> str:
    """Standardized error handling for Genie-related operations."""
    error_str = str(error).lower()
    
    if "permission" in error_str or "access" in error_str or "forbidden" in error_str:
        return f"❌ Access denied: {operation} failed due to insufficient permissions. Please check your workspace configuration or contact your administrator."
    elif "timeout" in error_str or "interrupted" in error_str:
        return f"⏱️ Operation timed out: {operation} took too long to complete. Please try again or contact support if the issue persists."
    elif "not found" in error_str or "does not exist" in error_str:
        return f"🔍 Resource not found: The requested resource for {operation} could not be found. Please verify your configuration."
    elif "network" in error_str or "connection" in error_str:
        return f"🌐 Network error: {operation} failed due to connectivity issues. Please check your network connection and try again."
    elif "quota" in error_str or "limit" in error_str:
        return f"📊 Resource limit: {operation} exceeded available resources. Please try again later or contact your administrator."
    else:
        return f"❌ {operation} failed: {str(error)}"


def create_error_response(error: Exception, context: str = "operation") -> Tuple[str, None, None, None]:
    """Create standardized error response tuple."""
    error_message = handle_genie_error(error, context)
    return error_message, None, None, None


def execute_with_fallback(primary_func, fallback_func, operation_name: str, *args, **kwargs):
    """Execute primary function with fallback on error."""
    try:
        return primary_func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"{operation_name} failed, attempting fallback: {e}")
        try:
            return fallback_func(*args, **kwargs)
        except Exception as fallback_error:
            logger.error(f"Both {operation_name} and fallback failed: {fallback_error}")
            raise fallback_error


def initialize_clients(bot_state) -> None:
    """Initialize Databricks and Slack clients."""
    try:
        logger.info("🔄 Initializing clients...")
        
        # Load configuration
        from .config import load_configuration, validate_environment
        load_configuration(bot_state)
        logger.info("✅ Configuration loaded")
        
        # Validate environment
        validate_environment(bot_state)
        logger.info("✅ Environment validated")
        
        # Initialize clients - use the workspace_client that was created in load_configuration()
        # Only create a new one if it wasn't already created
        if bot_state.workspace_client is None:
            bot_state.workspace_client = get_databricks_client(bot_state)
            logger.info("✅ Databricks workspace client initialized")
        else:
            logger.info("✅ Using existing Databricks workspace client")
        
        from .slack_handlers import get_slack_web_client, get_socket_mode_client, setup_slack_handlers
        bot_state.web_client = get_slack_web_client(bot_state)
        logger.info("✅ Slack WebClient initialized")
        
        # Create Socket Mode client for Slack
        bot_state.socket_mode_client = get_socket_mode_client(bot_state)
        logger.info("✅ Socket Mode client created")
        
        # Set up Slack event handlers
        setup_slack_handlers(bot_state)
        logger.info("✅ Slack event handlers set up")
        
        logger.info("✅ Successfully initialized all clients")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize clients: {e}")
        raise


def main() -> None:
    """Main function to start the enhanced Slackbot."""
    try:
        logger.info("🚀 Starting enhanced Slackbot backend...")
        logger.info(f"Environment: PORT={os.environ.get('PORT')}, DATABRICKS_HOST={os.environ.get('DATABRICKS_HOST')}")
        logger.info(f"Working directory: {os.getcwd()}")
        
        # Initialize clients
        initialize_clients(bot_state)
        logger.info("✅ Slackbot backend initialized successfully with performance enhancements")
        
        # Start Flask app
        port = int(os.environ.get('PORT', 8080))
        logger.info(f"🌐 Starting enhanced Flask web server on port {port}...")
        logger.info("   📈 Performance enhancements enabled:")
        logger.info("   • Connection pooling with health checks")
        logger.info("   • Enhanced memory management")
        logger.info("   • Real-time performance monitoring")
        logger.info("   • Large buffer sizes for data retrieval")
        
        # Start Socket Mode handler
        from .slack_handlers import start_socket_mode
        start_socket_mode(bot_state)
        logger.info("✅ Socket Mode handler started")
        
        # Wait a bit to ensure Socket Mode is fully connected
        time.sleep(5)
        
        # Verify Socket Mode is running
        if bot_state.socket_mode_client:
            try:
                if bot_state.socket_mode_client.is_connected():
                    logger.info("✅ Socket Mode client is running and connected")
                else:
                    logger.error("❌ Socket Mode client is not connected properly")
                    logger.error("Please check your SLACK_APP_TOKEN and ensure it starts with 'xapp-'")
            except AttributeError:
                logger.info("✅ Socket Mode client is initialized (connection status check not available)")
        
        # Start heartbeat thread for monitoring
        def heartbeat() -> None:
            while True:
                try:
                    current_time = time.time()
                    
                    # Calculate user-based queue statistics
                    total_queued_messages = sum(len(queue) for queue in bot_state.message_queue.values())
                    active_user_queues = len([q for q in bot_state.message_queue.values() if q])
                    
                    # Get performance metrics
                    try:
                        performance_metrics = bot_state.performance_monitor.get_metrics()
                        
                        logger.info(f"💓 Enhanced Heartbeat - Time: {datetime.now().isoformat()}")
                        logger.info(f"   📊 Performance - Avg Response: {performance_metrics.get('avg_response_time', 0):.2f}s, "
                                  f"Memory: {performance_metrics.get('memory_usage_mb', 0):.1f}MB, "
                                  f"Queries: {performance_metrics.get('query_count', 0)}")
                        logger.info(f"   📨 Queue - Active Users: {active_user_queues}, "
                                  f"Messages: {total_queued_messages}, "
                                  f"Processing Users: {len(bot_state.processing_users)}")
                        logger.info(f"   💬 Conversations: {len(bot_state.conversation_tracker)}")

                    except Exception as heartbeat_error:
                        logger.error(f"Error in enhanced heartbeat metrics: {heartbeat_error}")
                        # Basic heartbeat fallback
                        logger.info(f"💓 Basic Heartbeat - Time: {datetime.now().isoformat()}, "
                                  f"Conversations: {len(bot_state.conversation_tracker)}, "
                                  f"Processing Users: {len(bot_state.processing_users)}")
                    
                    time.sleep(Config.HEARTBEAT_INTERVAL)  # Heartbeat every minute
                except Exception as e:
                    logger.error(f"Enhanced heartbeat error: {e}")
                    time.sleep(Config.HEARTBEAT_INTERVAL)
        
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()
        logger.info("✅ Enhanced heartbeat monitoring started with performance metrics")
        
        # Set up graceful shutdown
        def signal_handler(sig, frame):
            logger.info("🛑 Received shutdown signal, shutting down...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        from .routes import app
        app.run(debug=False, host='0.0.0.0', port=port)
        
    except Exception as e:
        logger.error(f"❌ Failed to start enhanced Slackbot backend: {e}")
        raise





def extract_conversation_id_from_message(message_result: Any) -> Optional[str]:
    """Extract conversation ID from a Genie message result."""
    try:
        # Try various ways to get conversation ID
        if hasattr(message_result, 'conversation_id'):
            return str(message_result.conversation_id)
        elif hasattr(message_result, 'conversation') and hasattr(message_result.conversation, 'id'):
            return str(message_result.conversation.id)
        else:
            logger.warning("Could not extract conversation ID from message result")
            return None
    except Exception as e:
        logger.error(f"Error extracting conversation ID: {e}")
        return None


def process_query_attachment_optimized(workspace_client: WorkspaceClient, conv_id: str, message_result: Any,
                                     attachment_id: Optional[str], query_attachment: Any,
                                     genie_text_response: List[str], bot_state) -> Tuple[str, Optional[bytes], Optional[str], Optional[str]]:
    """Process query attachment using SDK best practices."""
    try:
        logger.info("Processing query attachment with SDK optimizations")
        
        response_parts = []
        response_parts.append(f"Genie generated a query: {query_attachment.description}")
        
        # Only show the SQL query if SHOW_SQL_QUERY is enabled
        if bot_state.show_sql_query:
            response_parts.append(f"Query: ```sql\n{query_attachment.query}\n```")
        
        # Try to execute query using proven fallback approach
        query_result = None
        if hasattr(message_result, 'id'):
            message_id = str(message_result.id)
            # Use the proven fallback query execution approach
            from .databricks_client import execute_query_with_fallback
            query_result = execute_query_with_fallback(
                workspace_client, str(bot_state.genie_space_id), conv_id, 
                message_id, attachment_id, query_attachment
            )
            if query_result:
                logger.info("Successfully executed query using fallback approach")
        
        # Handle query result if available
        if query_result:
            response_parts.append("✅ Query executed successfully")
            
            # Create CSV file from query results
            csv_bytes, filename, _ = create_csv_from_query_results(query_result, workspace_client, query_attachment.query)
            
            # Add preview of results to text response
            preview_text = format_query_results(query_result, workspace_client)
            response_parts.append(preview_text)
        else:
            # No query results available - provide guidance
            csv_bytes, filename = None, None
            response_parts.extend([
                "⚠️ *Query generated but could not be executed automatically.*",
                "You can copy the SQL query above and run it manually in your Databricks workspace.",
                "💡 **Common reasons for failure:**",
                "• Insufficient permissions for query execution",
                "• Genie space configuration issues",
                "• Query complexity or timeout",
                "• Contact your workspace administrator if needed"
            ])
        
        return "\n\n".join(response_parts), csv_bytes, filename, conv_id
        
    except Exception as e:
        logger.error(f"Error processing optimized query attachment: {e}")
        # Fallback to text-only response
        response_parts = []
        response_parts.append(f"Genie generated a query: {query_attachment.description}")
        
        if bot_state.show_sql_query:
            response_parts.append(f"Query: ```sql\n{query_attachment.query}\n```")
            
        response_parts.append(f"⚠️ Could not retrieve query results: {str(e)}")
        
        return "\n\n".join(response_parts), None, None, conv_id


def speak_with_genie_optimized(msg_input: str, workspace_client: WorkspaceClient, conversation_id: Optional[str] = None, bot_state=None) -> Tuple[str, Optional[bytes], Optional[str], Optional[str]]:
    """Optimized Genie interaction using SDK best practices and built-in waiters."""
    start_time = time.time()
    api_slot_acquired = False
    
    try:
        logger.info(f"Processing query with SDK waiters: {msg_input[:50]}...")
        
        # Wait for API slot and check rate limits
        if not wait_for_genie_api_slot(bot_state):
            return "⏳ System is at capacity. Please try again in a moment.", None, None, None
        
        api_slot_acquired = True
        
        if not check_genie_rate_limit(bot_state):
            return "⏳ Rate limit reached. Please wait a moment before trying again.", None, None, None
        
        # Use SDK's built-in waiters instead of custom polling
        try:
            if conversation_id:
                logger.info(f"Continuing conversation {conversation_id} with SDK waiter")
                # Use SDK's create_message_and_wait instead of manual polling
                message_result = workspace_client.genie.create_message_and_wait(
                    space_id=str(bot_state.genie_space_id),
                    conversation_id=conversation_id,
                    content=msg_input,
                    timeout=timedelta(minutes=10)  # SDK handles this properly
                )
                conv_id = conversation_id
            else:
                logger.info("Starting new conversation with SDK waiter")
                # Use SDK's start_conversation_and_wait instead of manual polling
                message_result = workspace_client.genie.start_conversation_and_wait(
                    space_id=str(bot_state.genie_space_id),
                    content=msg_input,
                    timeout=timedelta(minutes=10)
                )
                # Extract conversation ID from the message result
                conv_id = extract_conversation_id_from_message(message_result)
        
        except PermissionDenied as e:
            logger.error(f"Permission denied: {e}")
            return f"❌ Access denied: {e}", None, None, conversation_id
        except ResourceDoesNotExist as e:
            logger.error(f"Resource not found: {e}")
            return f"❌ Genie space not found: {e}", None, None, conversation_id
        except BadRequest as e:
            logger.error(f"Bad request: {e}")
            return f"❌ Invalid request: {e}", None, None, conversation_id
        except TooManyRequests as e:
            logger.error(f"Rate limit exceeded: {e}")
            return f"❌ Rate limit exceeded. Please try again later: {e}", None, None, conversation_id
        except InternalError as e:
            logger.error(f"Internal error: {e}")
            return f"❌ Databricks service temporarily unavailable: {e}", None, None, conversation_id
        
        if not message_result:
            return "Error: Failed to get message response from Genie.", None, None, conv_id

        # Extract and process content using existing logic
        genie_text_response, genie_query_attachment, attachment_id = extract_message_content(message_result)
        
        if genie_query_attachment:
            # Process query attachment with SDK best practices
            result = process_query_attachment_optimized(
                workspace_client, conv_id, message_result, attachment_id, 
                genie_query_attachment, genie_text_response, bot_state
            )
        else:
            # Return text-only response
            response_text = ' '.join(genie_text_response) if genie_text_response else 'No response from Genie.'
            
            # Clean up response - remove echoed input if it appears at the beginning
            if response_text.lower().startswith(msg_input.lower().strip()):
                cleaned_response = response_text[len(msg_input):].strip()
                if cleaned_response:
                    response_text = cleaned_response
            
            result = f"Genie: {response_text}", None, None, conv_id
        
        response_time = time.time() - start_time
        if bot_state and hasattr(bot_state, 'performance_monitor'):
            bot_state.performance_monitor.record_query(response_time, success=True)
        return result
        
    except Exception as e:
        logger.error(f"Error in optimized Genie communication: {e}")
        response_time = time.time() - start_time
        if bot_state and hasattr(bot_state, 'performance_monitor'):
            bot_state.performance_monitor.record_query(response_time, success=False)
        return create_error_response(e, "Genie communication")
    
    finally:
        # Always release the API slot
        if api_slot_acquired:
            release_genie_api_slot(bot_state)


if __name__ == '__main__':
    main() 
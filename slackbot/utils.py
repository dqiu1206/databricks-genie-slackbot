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
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from collections import OrderedDict
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient

from .config import Config
from .databricks_client import (
    DatabricksConnectionPool, get_databricks_client, return_databricks_client,
    wait_for_message_completion, wait_for_query_completion_if_needed,
    execute_query_with_fallback, extract_conversation_ids
)

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics for monitoring."""
    query_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_response_time: float = 0.0
    memory_usage_mb: float = 0.0
    active_connections: int = 0
    queue_depth: int = 0
    error_count: int = 0
    last_updated: datetime = field(default_factory=datetime.now)


class LRUCache:
    """Thread-safe LRU cache with TTL support for query results."""
    
    def __init__(self, max_size: int = Config.CACHE_SIZE, ttl: int = Config.CACHE_TTL):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = OrderedDict()
        self.timestamps = {}
        self.lock = RLock()
        self.hits = 0
        self.misses = 0
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._periodic_cleanup, daemon=True)
        self.cleanup_thread.start()
    
    def _is_expired(self, key: str) -> bool:
        """Check if a cache entry has expired."""
        if key not in self.timestamps:
            return True
        return time.time() - self.timestamps[key] > self.ttl
    
    def _periodic_cleanup(self):
        """Periodically clean up expired entries."""
        while True:
            try:
                time.sleep(Config.CLEANUP_INTERVAL)
                self._cleanup_expired()
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")
    
    def _cleanup_expired(self):
        """Remove expired entries from cache."""
        with self.lock:
            expired_keys = [k for k in self.cache.keys() if self._is_expired(k)]
            for key in expired_keys:
                del self.cache[key]
                del self.timestamps[key]
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            if key in self.cache and not self._is_expired(key):
                # Move to end (most recently used)
                value = self.cache.pop(key)
                self.cache[key] = value
                self.hits += 1
                return value
            else:
                if key in self.cache:
                    # Expired entry
                    del self.cache[key]
                    del self.timestamps[key]
                self.misses += 1
                return None
    
    def put(self, key: str, value: Any):
        """Put value in cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
            elif len(self.cache) >= self.max_size:
                # Remove least recently used
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]
                del self.timestamps[oldest_key]
            
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0
            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": hit_rate,
                "ttl": self.ttl
            }


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
    
    def record_cache_hit(self):
        """Record a cache hit."""
        with self.lock:
            self.metrics.cache_hits += 1
    
    def record_cache_miss(self):
        """Record a cache miss."""
        with self.lock:
            self.metrics.cache_misses += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        with self.lock:
            total_cache_requests = self.metrics.cache_hits + self.metrics.cache_misses
            cache_hit_rate = (self.metrics.cache_hits / total_cache_requests * 100) if total_cache_requests > 0 else 0
            
            return {
                "query_count": self.metrics.query_count,
                "cache_hits": self.metrics.cache_hits,
                "cache_misses": self.metrics.cache_misses,
                "cache_hit_rate": cache_hit_rate,
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
        self.slack_app: Optional[Any] = None
        self.socket_handler: Optional[Any] = None
        self.socket_thread: Optional[threading.Thread] = None
        self.bot_user_id: Optional[str] = None
        self.dbutils: Optional[Any] = None
        
        # Configuration
        self.databricks_host: Optional[str] = None
        self.genie_space_id: Optional[str] = None
        self.slack_app_token: Optional[str] = None
        self.slack_bot_token: Optional[str] = None
        self.show_sql_query: bool = True
        
        # Performance optimizations
        self.connection_pool = DatabricksConnectionPool()
        self.query_cache = LRUCache()
        self.performance_monitor = PerformanceMonitor()
        
        # Thread-safe data structures
        self.processed_messages = set()
        self.processed_event_ids = set()
        self.conversation_tracker = {}
        self.conversation_timestamps = {}
        
        # Message queuing for handling concurrent messages
        self.message_queue = {}  # channel_id -> queue of pending messages
        self.queue_locks = {}  # channel_id -> lock for queue operations
        self.processing_channels = set()  # channels currently being processed
        
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
        
        # Memory management
        self.last_memory_cleanup = time.time()
        self.memory_cleanup_interval = 300  # 5 minutes


# Global bot state
bot_state = BotState()


def check_memory_usage_and_cleanup(bot_state) -> None:
    """Monitor memory usage and trigger cleanup if needed."""
    try:
        try:
            import psutil
            process = psutil.Process()
            memory_percent = process.memory_percent()
            
            if memory_percent > Config.MEMORY_THRESHOLD:
                logger.warning(f"High memory usage detected: {memory_percent:.1f}%")
                trigger_cleanup(bot_state)
        except ImportError:
            logger.debug("psutil not available, skipping memory check")
            
    except Exception as e:
        logger.error(f"Error checking memory usage: {e}")


def trigger_cleanup(bot_state) -> None:
    """Trigger cleanup procedures."""
    try:
        # Clean up expired conversations
        cleanup_expired_conversations(bot_state)
        
        # Clean up processed messages
        cleanup_processed_messages(bot_state)
        
        # Force garbage collection
        import gc
        gc.collect()
        
        logger.info("Cleanup completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")


def generate_query_cache_key(query: str, space_id: str, conversation_id: Optional[str] = None) -> str:
    """Generate a cache key for query results."""
    # Create a deterministic key based on query content
    key_data = f"{query.strip().lower()}:{space_id}"
    if conversation_id:
        key_data += f":{conversation_id}"
    
    return hashlib.md5(key_data.encode()).hexdigest()


def should_cache_query(query: str) -> bool:
    """Determine if a query should be cached."""
    query_lower = query.lower()
    
    # Don't cache queries with current date/time functions
    non_cacheable_patterns = [
        'current_date', 'current_timestamp', 'now()', 'today()',
        'rand()', 'random()', 'uuid()'
    ]
    
    return not any(pattern in query_lower for pattern in non_cacheable_patterns)


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


def cleanup_expired_conversations(bot_state) -> None:
    """Clean up expired conversations to prevent memory leaks."""
    current_time = time.time()
    expired_keys = []
    
    with bot_state.conversation_lock:
        for conversation_key, last_used in bot_state.conversation_timestamps.items():
            if current_time - last_used >= Config.MAX_CONVERSATION_AGE:
                expired_keys.append(conversation_key)
        
        for key in expired_keys:
            logger.info(f"Cleaning up expired conversation for {key}")
            if key in bot_state.conversation_tracker:
                del bot_state.conversation_tracker[key]
            if key in bot_state.conversation_timestamps:
                del bot_state.conversation_timestamps[key]
    
    if expired_keys:
        logger.info(f"Cleaned up {len(expired_keys)} expired conversations")


def cleanup_processed_messages(bot_state) -> None:
    """Clean up processed messages to prevent memory leaks."""
    with bot_state.processed_messages_lock:
        if len(bot_state.processed_messages) > Config.MAX_PROCESSED_MESSAGES:
            bot_state.processed_messages = set(list(bot_state.processed_messages)[-500:])
            logger.info(f"Cleaned up processed messages, now tracking {len(bot_state.processed_messages)} messages")
    
    with bot_state.processed_event_ids_lock:
        if len(bot_state.processed_event_ids) > Config.MAX_PROCESSED_MESSAGES:
            bot_state.processed_event_ids = set(list(bot_state.processed_event_ids)[-500:])
            logger.info(f"Cleaned up processed event IDs, now tracking {len(bot_state.processed_event_ids)} events") 

    # Clean up empty queues
    cleanup_empty_queues(bot_state)


def cleanup_empty_queues(bot_state) -> None:
    """Clean up empty message queues to prevent memory leaks."""
    with bot_state.queue_lock:
        empty_channels = []
        for channel_id, queue in bot_state.message_queue.items():
            if not queue and channel_id not in bot_state.processing_channels:
                empty_channels.append(channel_id)
        
        for channel_id in empty_channels:
            del bot_state.message_queue[channel_id]
            if channel_id in bot_state.queue_locks:
                del bot_state.queue_locks[channel_id]
            logger.debug(f"Cleaned up empty queue for channel {channel_id}")
        
        if empty_channels:
            logger.info(f"Cleaned up {len(empty_channels)} empty message queues")


def get_channel_queue_lock(channel_id: str, bot_state) -> RLock:
    """Get or create a lock for a specific channel's message queue."""
    with bot_state.queue_lock:
        if channel_id not in bot_state.queue_locks:
            bot_state.queue_locks[channel_id] = RLock()
        return bot_state.queue_locks[channel_id]


def add_message_to_queue(channel_id: str, event: Dict[str, Any], say, client, bot_state) -> None:
    """Add a message to the channel's processing queue."""
    channel_lock = get_channel_queue_lock(channel_id, bot_state)
    
    with channel_lock:
        if channel_id not in bot_state.message_queue:
            bot_state.message_queue[channel_id] = []
        
        # Add message to queue
        bot_state.message_queue[channel_id].append((event, say, client))
        queue_length = len(bot_state.message_queue[channel_id])
        logger.info(f"📥 Added message to queue for channel {channel_id}. Queue length: {queue_length}")
        
        # Send queue status message if this is not the first message
        if queue_length > 1:
            try:
                position = queue_length - 1  # 0-based index
                say(f"⏳ *Message queued* - You are #{position} in line. I'll process your request as soon as possible!", thread_ts=event.get("ts"))
            except Exception as e:
                logger.error(f"Error sending queue status message: {e}")
        
        # Start processing if not already processing
        if channel_id not in bot_state.processing_channels:
            bot_state.processing_channels.add(channel_id)
            # Submit queue processing to thread pool
            bot_state.message_executor.submit(process_channel_queue, channel_id, bot_state)


def process_channel_queue(channel_id: str, bot_state) -> None:
    """Process all messages in a channel's queue sequentially."""
    channel_lock = get_channel_queue_lock(channel_id, bot_state)
    
    try:
        while True:
            with channel_lock:
                if not bot_state.message_queue.get(channel_id):
                    # No more messages in queue
                    break
                
                # Get next message from queue
                event, say, client = bot_state.message_queue[channel_id].pop(0)
                logger.info(f"📤 Processing message from queue for channel {channel_id}. Remaining in queue: {len(bot_state.message_queue[channel_id])}")
            
            # Process the message (outside the lock to avoid blocking)
            try:
                process_message_async(event, say, client, bot_state)
                # Small delay between messages to prevent overwhelming the system
                time.sleep(Config.MESSAGE_PROCESSING_DELAY)
            except Exception as e:
                logger.error(f"Error processing queued message for channel {channel_id}: {e}")
                # Send error response
                try:
                    say(f"❌ Sorry, I encountered an error processing your message: {str(e)}", thread_ts=event.get("ts"))
                except Exception as say_error:
                    logger.error(f"Error sending error message: {say_error}")
    
    finally:
        # Remove channel from processing set
        with bot_state.queue_lock:
            bot_state.processing_channels.discard(channel_id)
        logger.info(f"✅ Finished processing queue for channel {channel_id}")


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
    """Send a message to Databricks Genie and get a response with caching support."""
    start_time = time.time()
    
    try:
        # Check cache first for non-conversation queries
        cache_key = None
        if not conversation_id and should_cache_query(msg_input):
            cache_key = generate_query_cache_key(msg_input, str(bot_state.genie_space_id))
            cached_result = bot_state.query_cache.get(cache_key)
            
            if cached_result:
                bot_state.performance_monitor.record_cache_hit()
                logger.info(f"Cache hit for query: {msg_input[:50]}...")
                response_text, csv_bytes, filename, conv_id = cached_result
                response_time = time.time() - start_time
                bot_state.performance_monitor.record_query(response_time, success=True)
                
                # Add cache indicator to response
                if isinstance(response_text, str):
                    response_text += "\n\n*📚 Result retrieved from cache*"
                
                return response_text, csv_bytes, filename, conv_id
            else:
                bot_state.performance_monitor.record_cache_miss()

        logger.info(f"Processing query: {msg_input[:50]}...")
        
        # Start or continue conversation
        if conversation_id:
            logger.info(f"Continuing conversation {conversation_id}")
            conv_response = workspace_client.genie.create_message(
                space_id=str(bot_state.genie_space_id),
                conversation_id=conversation_id,
                content=msg_input
            )
            message_id = getattr(conv_response, 'id', None) or getattr(conv_response, 'message_id', None)
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
            logger.error(f"Missing conversation_id or message_id in response")
            return "Error: Could not retrieve conversation details from Genie's response.", None, None, conv_id

        # Wait for message completion and process result
        message_result = wait_for_message_completion(workspace_client, str(bot_state.genie_space_id), conv_id, message_id)
        
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
            result = f"Genie: {response_text}", None, None, conv_id
        
        # Cache the result if applicable
        if cache_key and result and should_cache_query(msg_input):
            bot_state.query_cache.put(cache_key, result)
            logger.debug(f"Cached query result with key: {cache_key}")
        
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=True)
        return result
        
    except Exception as e:
        logger.error(f"Error in Genie communication: {e}")
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=False)
        return create_error_response(e, "Genie communication")


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
        logger.debug(f"🔍 Processing event - Type: {event_type}, TS: {event_ts}, Event ID: {event_id}")
        
        # Check if this is a bot message
        if is_bot_message(event, bot_state):
            logger.debug("Ignoring bot message to prevent loops")
            return
        
        # Get message content
        message_content = event.get("text", "")
        if not message_content:
            logger.debug("Received empty message, ignoring")
            return
        
        # Check for duplicates
        if is_duplicate_message(event, message_content, bot_state):
            return
        
        # Check for bot response patterns
        if is_bot_response_pattern(message_content):
            logger.debug("Ignoring message that starts with bot response pattern to prevent loops")
            return
        
        # Extract channel information
        channel_id = event.get("channel", "unknown_channel")
        thread_ts = event.get('thread_ts')
        message_ts = event.get("ts")
        user_id = event.get("user", "unknown_user")
        
        # Handle special commands
        if handle_special_commands(message_content, channel_id, thread_ts, message_ts, say, bot_state):
            return
        
        # Log message information
        logger.info(f"📨 Message received from user {user_id} in channel {channel_id}")
        logger.debug(f"   Event type: {event_type}")
        logger.debug(f"   Timestamp: {message_ts}")
        logger.debug(f"   Thread timestamp: {thread_ts}")
        logger.debug(f"   Message content: {message_content[:100]}{'...' if len(message_content) > 100 else ''}")

        # Clean up processed messages to prevent memory leaks
        cleanup_processed_messages(bot_state)

        # Get workspace client from connection pool
        workspace_client = None
        try:
            workspace_client = get_databricks_client(bot_state)
        except Exception as e:
            logger.error(f"Failed to get Databricks client: {e}")
            say("❌ Databricks client not available. Please check the service configuration.", thread_ts=message_ts)
            return
        
        try:
            # Check for existing conversation in this thread
            existing_conversation_id = get_active_conversation_id(channel_id, thread_ts, message_ts)
            
            if existing_conversation_id:
                logger.info(f"🔄 Continuing existing conversation {existing_conversation_id} in thread {thread_ts or 'new'} for user {user_id} in channel {channel_id}")
            else:
                logger.info(f"🆕 Starting new conversation in thread {thread_ts or 'new'} for user {user_id} in channel {channel_id}")
            
            # Clean up expired conversations and check memory periodically
            cleanup_expired_conversations(bot_state)
            
            # Check memory usage and trigger cleanup if needed
            if time.time() - bot_state.last_memory_cleanup > bot_state.memory_cleanup_interval:
                check_memory_usage_and_cleanup(bot_state)
                bot_state.last_memory_cleanup = time.time()
            
            # Get response from Genie (continue existing or start new)
            try:
                genie_response, csv_bytes, filename, conversation_id = speak_with_genie(
                    message_content, 
                    workspace_client, 
                    existing_conversation_id,  # Use existing conversation if available
                    bot_state
                )
            except Exception as genie_error:
                logger.error(f"Error in speak_with_genie: {genie_error}")
                genie_response = f"❌ Sorry, I encountered an error while processing your request: {str(genie_error)}"
                csv_bytes, filename, conversation_id = None, None, None
        
        finally:
            # Always return the workspace client to the pool
            if workspace_client:
                return_databricks_client(workspace_client, bot_state)
        
        # Store the conversation ID if we got one (for new conversations or to update existing)
        if conversation_id:
            store_conversation_id(channel_id, thread_ts, conversation_id, message_ts)
            if existing_conversation_id:
                logger.info(f"✅ Continued conversation {conversation_id}")
            else:
                logger.info(f"✅ Started new conversation {conversation_id}")

        # Send text response back to Slack
        logger.info(f"📤 Sending response to user {user_id} in channel {channel_id}")
        logger.debug(f"   Response length: {len(genie_response)} characters")
        logger.debug(f"   Response preview: {genie_response[:100]}{'...' if len(genie_response) > 100 else ''}")
        
        say(genie_response, thread_ts=message_ts)
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
                    
                    # Upload file to Slack
                    response = client.files_upload_v2(
                        channel=event.get("channel"),
                        thread_ts=event.get("ts"),
                        file=csv_bytes,
                        filename=filename,
                        title=f"Query Results - {filename}",
                        initial_comment="📁 Here's your query results file:"
                    )
                    logger.info(f"✅ Successfully uploaded file {filename} to user {user_id}")
                    
                    # Add a confirmation message
                    say(f"✅ *File uploaded successfully!* {filename} ({size_str})", thread_ts=message_ts)
                    
                except Exception as upload_error:
                    logger.error(f"Error uploading file to Slack: {upload_error}")
                    # If upload fails, provide error message
                    say(f"📁 *File upload failed: {str(upload_error)}*", thread_ts=message_ts)
            else:
                # File too large, provide guidance
                say(f"📁 *File too large for Slack ({size_str}). Please try a more specific query with LIMIT clause.*", thread_ts=message_ts)
        
        # Mark this message as processed
        message_hash = hashlib.md5(message_content.encode()).hexdigest()[:8]
        message_key = (user_id, message_ts, message_hash)
        
        with bot_state.processed_messages_lock:
            bot_state.processed_messages.add(message_key)
        
        # Mark event ID as processed
        with bot_state.processed_event_ids_lock:
            if event_id != "no_event_id":
                bot_state.processed_event_ids.add(event_id)
        
        logger.debug(f"✅ Marked message as processed - Message key: {message_key}, Event ID: {event_id}")
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
        
        from .slack_handlers import get_slack_app, get_socket_handler, setup_slack_handlers
        bot_state.slack_app = get_slack_app(bot_state)
        logger.info("✅ Slack app initialized")
        
        # Create Socket Mode handler for Slack
        bot_state.socket_handler = get_socket_handler(bot_state.slack_app, bot_state)
        logger.info("✅ Socket Mode handler created")
        
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
        
        # Initialize clients
        initialize_clients(bot_state)
        logger.info("✅ Slackbot backend initialized successfully with performance enhancements")
        
        # Start Socket Mode handler
        from .slack_handlers import start_socket_mode
        start_socket_mode(bot_state)
        logger.info("✅ Socket Mode handler started")
        
        # Start heartbeat thread for monitoring
        def heartbeat() -> None:
            while True:
                try:
                    current_time = time.time()
                    
                    # Calculate queue statistics
                    total_queued_messages = sum(len(queue) for queue in bot_state.message_queue.values())
                    active_queues = len([q for q in bot_state.message_queue.values() if q])
                    
                    # Get performance metrics
                    try:
                        performance_metrics = bot_state.performance_monitor.get_metrics()
                        connection_stats = bot_state.connection_pool.get_stats()
                        cache_stats = bot_state.query_cache.get_stats()
                        
                        logger.info(f"💓 Enhanced Heartbeat - Time: {datetime.now().isoformat()}")
                        logger.info(f"   📊 Performance - Avg Response: {performance_metrics.get('avg_response_time', 0):.2f}s, "
                                  f"Memory: {performance_metrics.get('memory_usage_mb', 0):.1f}MB, "
                                  f"Queries: {performance_metrics.get('query_count', 0)}")
                        logger.info(f"   🔗 Connections - Pool: {connection_stats.get('pool_size', 0)}/{connection_stats.get('max_size', 0)}, "
                                  f"Created: {connection_stats.get('created_connections', 0)}, "
                                  f"Reused: {connection_stats.get('reused_connections', 0)}")
                        logger.info(f"   📚 Cache - Size: {cache_stats.get('size', 0)}/{cache_stats.get('max_size', 0)}, "
                                  f"Hit Rate: {cache_stats.get('hit_rate', 0):.1f}%")
                        logger.info(f"   📨 Queue - Active: {active_queues}, "
                                  f"Messages: {total_queued_messages}, "
                                  f"Processing: {len(bot_state.processing_channels)}")
                        logger.info(f"   💬 Conversations: {len(bot_state.conversation_tracker)}")
                        
                        # Periodic cleanup check
                        if performance_metrics.get('memory_usage_mb', 0) > Config.MEMORY_THRESHOLD:
                            logger.warning("🧹 Triggering cleanup due to high memory usage")
                            trigger_cleanup(bot_state)
                    except Exception as heartbeat_error:
                        logger.error(f"Error in enhanced heartbeat metrics: {heartbeat_error}")
                        # Basic heartbeat fallback
                        logger.info(f"💓 Basic Heartbeat - Time: {datetime.now().isoformat()}, "
                                  f"Conversations: {len(bot_state.conversation_tracker)}, "
                                  f"Processing: {len(bot_state.processing_channels)}")
                    
                    time.sleep(Config.HEARTBEAT_INTERVAL)  # Heartbeat every minute
                except Exception as e:
                    logger.error(f"Enhanced heartbeat error: {e}")
                    time.sleep(Config.HEARTBEAT_INTERVAL)
        
        heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
        heartbeat_thread.start()
        logger.info("✅ Enhanced heartbeat monitoring started with performance metrics")
        
        # Set up graceful shutdown
        def signal_handler(sig, frame):
            logger.info("🛑 Received shutdown signal, cleaning up...")
            cleanup_and_shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start Flask app
        logger.info("🌐 Starting enhanced Flask web server on port 8080...")
        logger.info("   📈 Performance enhancements enabled:")
        logger.info("   • Connection pooling with health checks")
        logger.info("   • Intelligent query result caching")
        logger.info("   • Enhanced memory management")
        logger.info("   • Real-time performance monitoring")
        logger.info("   • Large buffer sizes for data retrieval")
        
        from .routes import app
        app.run(debug=False, host='0.0.0.0', port=8080)
        
    except Exception as e:
        logger.error(f"❌ Failed to start enhanced Slackbot backend: {e}")
        cleanup_and_shutdown()
        raise


def cleanup_and_shutdown():
    """Perform cleanup operations before shutdown."""
    try:
        logger.info("🧹 Starting graceful shutdown cleanup...")
        
        # Close all database connections
        bot_state.connection_pool.close_all()
        logger.info("✅ Closed all database connections")
        
        # Shutdown thread pool
        if bot_state.message_executor:
            bot_state.message_executor.shutdown(wait=True)
            logger.info("✅ Shutdown message executor thread pool")
        
        # Close Socket Mode handler
        if bot_state.socket_handler:
            try:
                if hasattr(bot_state.socket_handler, 'close'):
                    bot_state.socket_handler.close()
                    logger.info("✅ Closed Socket Mode handler")
                else:
                    logger.info("✅ Socket Mode handler doesn't have close method")
            except Exception as e:
                logger.warning(f"Error closing Socket Mode handler: {e}")
        
        # Final performance report
        try:
            performance_metrics = bot_state.performance_monitor.get_metrics()
            connection_stats = bot_state.connection_pool.get_stats()
            cache_stats = bot_state.query_cache.get_stats()
            
            logger.info("📊 Final Performance Report:")
            logger.info(f"   • Total queries processed: {performance_metrics.get('query_count', 0)}")
            logger.info(f"   • Average response time: {performance_metrics.get('avg_response_time', 0):.2f}s")
            logger.info(f"   • Cache hit rate: {cache_stats.get('hit_rate', 0):.1f}%")
            logger.info(f"   • Connections created: {connection_stats.get('created_connections', 0)}")
            logger.info(f"   • Connections reused: {connection_stats.get('reused_connections', 0)}")
            logger.info(f"   • Error count: {performance_metrics.get('error_count', 0)}")
        except Exception as report_error:
            logger.warning(f"Error generating final performance report: {report_error}")
            logger.info("📊 Basic Final Report: Shutdown completed")
        
        logger.info("✅ Graceful shutdown completed")
        
    except Exception as e:
        logger.error(f"Error during shutdown cleanup: {e}")


if __name__ == '__main__':
    main() 
"""
Flask routes module for Databricks Genie Slack Bot.

This module contains all Flask routes and API endpoints for the web application.
"""

import time
import logging
from flask import Flask, request, jsonify

from .config import Config
from .databricks_client import get_databricks_client, return_databricks_client
from .utils import (
    speak_with_genie, check_memory_usage_and_cleanup, cleanup_expired_conversations,
    LRUCache
)

logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)


@app.route('/')
def health_check():
    """Health check endpoint with performance metrics."""
    from .utils import bot_state
    connection_stats = bot_state.connection_pool.get_stats()
    cache_stats = bot_state.query_cache.get_stats()
    performance_metrics = bot_state.performance_monitor.get_metrics()
    
    return jsonify({
        'status': 'healthy',
        'service': 'slackbot-backend',
        'databricks_connected': len(bot_state.connection_pool.pool) > 0 or bot_state.workspace_client is not None,
        'slack_connected': bot_state.slack_app is not None,
        'socket_mode_active': bot_state.socket_handler is not None,
        'performance': {
            'connection_pool': connection_stats,
            'query_cache': cache_stats,
            'metrics': performance_metrics
        }
    })


@app.route('/api/chat', methods=['POST'])
def chat_endpoint():
    """Direct API endpoint for chat functionality with caching."""
    from .utils import bot_state
    start_time = time.time()
    workspace_client = None
    
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({'error': 'Message is required'}), 400
        
        message = data['message']
        
        # Get workspace client from pool
        workspace_client = get_databricks_client(bot_state)
        
        # Get response from Genie (API endpoint always starts fresh conversations)
        genie_response, csv_bytes, filename, conversation_id = speak_with_genie(message, workspace_client, None, bot_state)
        
        response_data = {
            'response': genie_response,
            'has_csv': csv_bytes is not None,
            'filename': filename,
            'cached': False  # This would be set by speak_with_genie_cached
        }
        
        # If CSV data is available and requested, include it
        if csv_bytes and data.get('include_csv', False):
            import base64
            response_data['csv_data'] = base64.b64encode(csv_bytes).decode('utf-8')
        
        # Record performance metrics
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=True)
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=False)
        return jsonify({'error': str(e)}), 500
    
    finally:
        # Return client to pool
        if workspace_client:
            return_databricks_client(workspace_client, bot_state)





@app.route('/api/status')
def status():
    """Get detailed status of the service with performance metrics."""
    from .utils import bot_state
    # Check memory and trigger cleanup if needed
    check_memory_usage_and_cleanup(bot_state)
    
    # Clean up expired conversations before reporting status
    cleanup_expired_conversations(bot_state)
    
    # Calculate queue statistics
    total_queued_messages = sum(len(queue) for queue in bot_state.message_queue.values())
    active_queues = len([q for q in bot_state.message_queue.values() if q])
    
    # Get performance metrics
    connection_stats = bot_state.connection_pool.get_stats()
    cache_stats = bot_state.query_cache.get_stats()
    performance_metrics = bot_state.performance_monitor.get_metrics()
    
    return jsonify({
        'status': 'running',
        'service': 'slackbot-backend',
        'databricks_host': bot_state.databricks_host,
        'genie_space_id': bot_state.genie_space_id,
        'databricks_connected': len(bot_state.connection_pool.pool) > 0 or bot_state.workspace_client is not None,
        'slack_connected': bot_state.slack_app is not None,
        'socket_mode_active': bot_state.socket_handler is not None,
        'socket_thread_running': bot_state.socket_thread is not None and bot_state.socket_thread.is_alive(),
        'conversation_stats': {
            'active_conversations': len(bot_state.conversation_tracker),
            'max_conversation_age_seconds': Config.MAX_CONVERSATION_AGE
        },
        'queue_stats': {
            'processing_channels': len(bot_state.processing_channels),
            'active_queues': active_queues,
            'total_queued_messages': total_queued_messages,
            'queue_details': {
                channel_id: len(queue) for channel_id, queue in bot_state.message_queue.items() if queue
            }
        },
        'performance': {
            'connection_pool': connection_stats,
            'query_cache': cache_stats,
            'metrics': performance_metrics
        }
    })


@app.route('/api/performance')
def performance_metrics():
    """Get detailed performance metrics."""
    from .utils import bot_state
    connection_stats = bot_state.connection_pool.get_stats()
    cache_stats = bot_state.query_cache.get_stats()
    performance_metrics = bot_state.performance_monitor.get_metrics()
    
    return jsonify({
        'connection_pool': connection_stats,
        'query_cache': cache_stats,
        'system_metrics': performance_metrics,
        'optimizations': {
            'connection_pooling': True,
            'query_caching': True,
            'memory_monitoring': True,
            'enhanced_cleanup': True,
            'buffer_optimization': True
        }
    })


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """Clear the query cache (admin endpoint)."""
    from .utils import bot_state
    try:
        old_stats = bot_state.query_cache.get_stats()
        bot_state.query_cache = LRUCache()  # Reset cache
        new_stats = bot_state.query_cache.get_stats()
        
        return jsonify({
            'status': 'cache_cleared',
            'old_cache_size': old_stats['size'],
            'new_cache_size': new_stats['size']
        })
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return jsonify({'error': str(e)}), 500 
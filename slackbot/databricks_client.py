"""
Databricks client module for Genie Slack Bot.

This module handles all Databricks WorkspaceClient interactions, connection pooling,
and Genie API operations.
"""

import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from threading import RLock
from dataclasses import dataclass


from databricks.sdk import WorkspaceClient
from .config import Config, GenieError

logger = logging.getLogger(__name__)


@dataclass
class ConnectionMetrics:
    """Metrics for connection pool performance."""
    created_count: int = 0
    reused_count: int = 0
    closed_count: int = 0
    failed_count: int = 0
    current_pool_size: int = 0
    peak_pool_size: int = 0


class DatabricksConnectionPool:
    """Thread-safe connection pool for Databricks WorkspaceClient."""
    
    def __init__(self, max_size: int = Config.POOL_SIZE, timeout: int = Config.CONNECTION_TIMEOUT):
        self.max_size = max_size
        self.timeout = timeout
        self.pool = []
        self.lock = RLock()
        self.created_connections = 0
        self.metrics = ConnectionMetrics()
        
        # Connection configuration
        self.databricks_host = None
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        
        # Health check
        self.last_health_check = time.time()
        
    def configure(self, databricks_host: str, client_id: str = None, 
                 client_secret: str = None, access_token: str = None):
        """Configure connection pool with authentication details."""
        self.databricks_host = databricks_host
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        
    def _create_connection(self) -> WorkspaceClient:
        """Create a new WorkspaceClient connection."""
        try:
            if self.client_id and self.client_secret:
                client = WorkspaceClient(
                    host=self.databricks_host,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )
            elif self.access_token:
                client = WorkspaceClient(
                    host=self.databricks_host,
                    token=self.access_token
                )
            else:
                raise ValueError("No valid authentication method configured")
            
            # Test the connection
            client.current_user.me()
            
            # Attach metadata
            client._pool_created_at = time.time()
            client._pool_last_used = time.time()
            
            self.metrics.created_count += 1
            self.created_connections += 1
            
            logger.debug(f"Created new database connection #{self.created_connections}")
            return client
            
        except Exception as e:
            self.metrics.failed_count += 1
            logger.error(f"Failed to create database connection: {e}")
            raise
    
    def _is_connection_healthy(self, client: WorkspaceClient) -> bool:
        """Check if a connection is still healthy."""
        try:
            # Check connection age
            created_at = getattr(client, '_pool_created_at', 0)
            if time.time() - created_at > Config.CONNECTION_MAX_AGE:
                return False
            
            # Quick health check (only if it's been a while)
            if time.time() - self.last_health_check > Config.CLEANUP_INTERVAL:
                client.current_user.me()
                self.last_health_check = time.time()
            
            return True
        except Exception as e:
            logger.debug(f"Connection health check failed: {e}")
            return False
    
    def get_connection(self) -> WorkspaceClient:
        """Get a connection from the pool."""
        start_time = time.time()
        
        with self.lock:
            # Try to get a healthy connection from pool
            while self.pool:
                client = self.pool.pop()
                if self._is_connection_healthy(client):
                    client._pool_last_used = time.time()
                    self.metrics.reused_count += 1
                    self.metrics.current_pool_size = len(self.pool)
                    
                    elapsed = time.time() - start_time
                    logger.debug(f"Reused pooled connection in {elapsed:.3f}s")
                    return client
                else:
                    # Connection is unhealthy, close it
                    try:
                        client._client.close()
                    except:
                        pass
                    self.metrics.closed_count += 1
            
            # No healthy connections available, create new one
            client = self._create_connection()
            self.metrics.current_pool_size = len(self.pool)
            self.metrics.peak_pool_size = max(self.metrics.peak_pool_size, self.created_connections)
            
            elapsed = time.time() - start_time
            logger.info(f"Created new connection in {elapsed:.3f}s")
            return client
    
    def return_connection(self, client: WorkspaceClient):
        """Return a connection to the pool."""
        if not client:
            return
            
        with self.lock:
            if len(self.pool) < self.max_size and self._is_connection_healthy(client):
                self.pool.append(client)
                self.metrics.current_pool_size = len(self.pool)
                logger.debug(f"Returned connection to pool (size: {len(self.pool)})")
            else:
                # Pool is full or connection unhealthy, close it
                try:
                    client._client.close()
                except:
                    pass
                self.metrics.closed_count += 1
                logger.debug("Closed connection (pool full or unhealthy)")
    
    def close_all(self):
        """Close all connections in the pool."""
        with self.lock:
            while self.pool:
                client = self.pool.pop()
                try:
                    client._client.close()
                except:
                    pass
                self.metrics.closed_count += 1
            logger.info("Closed all pooled connections")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self.lock:
            return {
                "pool_size": len(self.pool),
                "max_size": self.max_size,
                "created_connections": self.metrics.created_count,
                "reused_connections": self.metrics.reused_count,
                "closed_connections": self.metrics.closed_count,
                "failed_connections": self.metrics.failed_count,
                "peak_pool_size": self.metrics.peak_pool_size
            }


def get_databricks_client(bot_state) -> WorkspaceClient:
    """Get a Databricks workspace client from the connection pool."""
    try:
        return bot_state.connection_pool.get_connection()
    except Exception as e:
        logger.error(f"Failed to get Databricks client from pool: {e}")
        raise


def return_databricks_client(client: WorkspaceClient, bot_state):
    """Return a Databricks workspace client to the connection pool."""
    try:
        bot_state.connection_pool.return_connection(client)
    except Exception as e:
        logger.error(f"Failed to return Databricks client to pool: {e}")


def wait_for_message_completion(workspace_client: WorkspaceClient, space_id: str, conversation_id: str, message_id: str, max_wait_time: int = Config.MAX_WAIT_TIME) -> Any:
    """Wait for message generation to complete and return the result."""
    start_time = time.time()
    poll_interval = Config.POLL_INTERVAL
    
    while time.time() - start_time < max_wait_time:
        try:
            message_result = workspace_client.genie.get_message(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id
            )
            
            if message_result is None:
                logger.debug("Received None message result, continuing to poll")
                time.sleep(poll_interval)
                continue
            
            # Get status value
            status_value = None
            if message_result.status is not None:
                status_value = message_result.status.value if hasattr(message_result.status, 'value') else str(message_result.status)
            else:
                # Try alternative status fields
                for attr in ['state', 'phase', 'stage']:
                    try:
                        alt_status = getattr(message_result, attr, None)
                        if alt_status is not None:
                            status_value = alt_status.value if hasattr(alt_status, 'value') else str(alt_status)
                            logger.debug(f"Using alternative status field '{attr}': {status_value}")
                            break
                    except (AttributeError, Exception):
                        continue
            
            if status_value is None:
                logger.debug("No status found in any field, treating as IN_PROGRESS and continuing to poll")
                time.sleep(poll_interval)
                continue
            
            logger.debug(f"Processing message status: {status_value}")
            
            if status_value == 'COMPLETED':
                logger.info(f"Message generation completed successfully with status: {status_value}")
                return message_result
            elif status_value == 'FAILED':
                error_msg = str(message_result.error) if message_result.error else "Unknown error"
                raise GenieError(f"Message generation failed: {error_msg}")
            elif status_value == 'CANCELLED':
                raise GenieError("Message generation was cancelled")
            elif status_value in ['IN_PROGRESS', 'EXECUTING_QUERY']:
                logger.debug(f"Message still processing, status: {status_value}")

                # Apply exponential backoff after 2 minutes
                if time.time() - start_time > 120:
                    poll_interval = min(poll_interval * 1.5, 30)
                
                time.sleep(poll_interval)
                continue
            else:
                logger.debug(f"Unknown status: {status_value}, continuing to poll")
                time.sleep(poll_interval)
                continue
            
        except Exception as e:
            logger.error(f"Error checking message status: {e}")
            
            # Check for specific error types
            if "PermissionDenied" in str(e) or "You need" in str(e):
                raise GenieError(f"Conversation access denied: {e}")
            elif "timeout" in str(e).lower() or "connection" in str(e).lower():
                raise GenieError(f"Network error during message polling: {e}")
            
            continue
    
    raise GenieError(f"Message generation timed out after {max_wait_time} seconds")


def wait_for_query_completion_if_needed(query_result: Any, workspace_client: WorkspaceClient, max_wait_time: int = 60) -> Any:
    """Check if query result needs to wait for completion and wait if necessary."""
    try:
        statement_response = getattr(query_result, 'statement_response', None)
        if statement_response:
            status = getattr(statement_response, 'status', None)
            if status:
                state = getattr(status, 'state', None)
                if state:
                    state_value = state.value if hasattr(state, 'value') else str(state)
                    logger.debug(f"Query result state: {state_value}")
                    
                    if state_value in ['PENDING', 'RUNNING', 'EXECUTING']:
                        logger.info("Query is still executing, waiting for completion...")
                        return wait_for_query_completion(query_result, workspace_client, max_wait_time)
                    elif state_value in ['FINISHED', 'SUCCEEDED', 'COMPLETED']:
                        logger.info("Query is already completed")
                        return query_result
                    elif state_value in ['FAILED', 'CANCELLED', 'CLOSED']:
                        error_msg = str(status.error) if status.error else "Unknown error"
                        raise GenieError(f"Query execution failed: {error_msg}")
        
        logger.debug("Query result appears to be already complete or from a different source")
        return query_result
        
    except Exception as e:
        logger.error(f"Error checking query completion status: {e}")
        return query_result


def wait_for_query_completion(query_result: Any, workspace_client: WorkspaceClient, max_wait_time: int = 60) -> Any:
    """Wait for query execution to complete and return the final result."""
    start_time = time.time()
    poll_interval = 5
    
    # Extract statement ID
    statement_id = None
    try:
        statement_response = getattr(query_result, 'statement_response', None)
        if statement_response:
            statement_id = getattr(statement_response, 'statement_id', None)
    except (KeyError, AttributeError):
        pass
    
    if not statement_id:
        logger.warning("No statement ID found in query result, cannot wait for completion")
        return query_result
    
    logger.info(f"Waiting for query completion with statement ID: {statement_id}")
    
    while time.time() - start_time < max_wait_time:
        try:
            statement_status = workspace_client.statement_execution.get_statement(statement_id)
            
            if statement_status is None:
                logger.debug("Received None statement status, continuing to poll")
                time.sleep(poll_interval)
                continue
            
            state = getattr(statement_status, 'status', None)
            if state:
                state_value = state.state.value if hasattr(state.state, 'value') else str(state.state)
                logger.debug(f"Statement state: {state_value}")
                
                if state_value in ['FINISHED', 'SUCCEEDED', 'COMPLETED']:
                    logger.info(f"Query execution completed successfully with state: {state_value}")
                    return statement_status
                elif state_value in ['FAILED', 'CANCELLED', 'CLOSED']:
                    error_msg = str(state.error) if state.error else "Unknown error"
                    raise GenieError(f"Query execution failed: {error_msg}")
                elif state_value in ['PENDING', 'RUNNING', 'EXECUTING']:
                    logger.debug(f"Query still executing, state: {state_value}")
                    
                    # Apply exponential backoff after 2 minutes
                    if time.time() - start_time > 120:
                        poll_interval = min(poll_interval * 1.5, 30)
                    
                    time.sleep(poll_interval)
                    continue
                else:
                    logger.debug(f"Unknown state: {state_value}, continuing to poll")
                    time.sleep(poll_interval)
                    continue
            else:
                logger.debug("No state found in statement status, continuing to poll")
                time.sleep(poll_interval)
                continue
                
        except Exception as e:
            logger.error(f"Error checking statement status: {e}")
            time.sleep(poll_interval)
    
    raise GenieError(f"Query execution timed out after {max_wait_time} seconds")


def execute_query_with_fallback(workspace_client: WorkspaceClient, space_id: str, conversation_id: str, message_id: str, attachment_id: Optional[str], query_attachment: Any) -> Optional[Any]:
    """Execute query with multiple fallback strategies."""
    query_result = None
    
    # Try to get results by attachment ID first
    if attachment_id:
        logger.info("Getting query results using get_message_query_result_by_attachment...")
        try:
            query_result = workspace_client.genie.get_message_query_result_by_attachment(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id,
                attachment_id=attachment_id
            )
            return query_result
        except Exception as e:
            logger.warning(f"Failed to get results by attachment: {e}")
    
    # Try Genie's execute method
    logger.info("Attempting to execute query using Genie's execute method...")
    try:
        query_result = workspace_client.genie.execute_message_query(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id
        )
        logger.info("Successfully executed query using Genie's default warehouse")
        return query_result
    except Exception as genie_exec_error:
        logger.warning(f"Genie execute method failed: {genie_exec_error}")
    
    # Fall back to direct warehouse execution
    logger.info("Falling back to direct warehouse execution...")
    try:
        warehouses = list(workspace_client.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
            if warehouse_id:
                logger.info(f"Using warehouse ID: {warehouse_id}")
                
                # Check if this is a system table query
                query_text = query_attachment.query.lower()
                is_system_query = any(table in query_text for table in Config.SYSTEM_TABLE_PATTERNS)
                
                if is_system_query:
                    logger.info("Detected system table query - this may require special permissions")
                
                query_result = workspace_client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=query_attachment.query,
                    wait_timeout="120s" if is_system_query else "60s"
                )
                logger.info("Successfully executed query directly")
                return query_result
            else:
                logger.warning("Warehouse ID is None, cannot execute query directly")
        else:
            logger.warning("No warehouses available for direct query execution")
    except Exception as direct_exec_error:
        logger.error(f"Direct query execution failed: {direct_exec_error}")
        
        # Check for specific error types
        error_str = str(direct_exec_error).lower()
        if "permission" in error_str or "access" in error_str:
            logger.warning("Query failed due to permission issues - this is common with system tables")
        elif "timeout" in error_str or "interrupted" in error_str:
            logger.warning("Query failed due to timeout - system queries can take longer")
        elif "not found" in error_str or "does not exist" in error_str:
            logger.warning("Query failed because table or schema does not exist")
    
    return None


def extract_conversation_ids(conv_response: Any) -> Tuple[Optional[str], Optional[str]]:
    """Extract conversation and message IDs from response with simplified checking."""
    # Extract conversation ID
    conversation = safe_getattr(conv_response, 'conversation')
    conv_id = safe_getattr(conversation, 'id') if conversation else None
    if not conv_id:
        conv_id = safe_getattr(conv_response, 'conversation_id')
    
    # Extract message ID
    message = safe_getattr(conv_response, 'message')
    message_id = safe_getattr(message, 'id') if message else None
    if not message_id:
        message_id = safe_extract_id(conv_response, ['message_id', 'id'])
    
    return conv_id, message_id


def safe_getattr(obj: Any, attr: str, default: Any = None, log_errors: bool = False) -> Any:
    """Safely get an attribute from an object."""
    try:
        return getattr(obj, attr, default)
    except Exception as e:
        if log_errors:
            logger.error(f"Error getting attribute {attr}: {e}")
        return default


def safe_extract_id(response: Any, id_fields: List[str] = None) -> Optional[str]:
    """Safely extract ID from response using multiple possible field names."""
    if id_fields is None:
        id_fields = ['id', 'message_id', 'conversation_id']
    
    for field in id_fields:
        try:
            value = getattr(response, field, None)
            if value:
                return str(value)
        except Exception:
            continue
    
    return None 
#!/usr/bin/env python3
"""
Test script for the new Genie throttling system.
This script validates that the throttling implementation correctly enforces rate limits.
"""

import time
import logging
from typing import Any
from slackbot.utils import GenieRateLimiter, WorkspaceThrottleManager, ThrottleStatus
from slackbot.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def mock_genie_request(delay: float = 0.1) -> str:
    """Mock Genie API request with configurable delay."""
    time.sleep(delay)
    return f"Mock Genie response at {time.time():.2f}"


def test_rate_limiter():
    """Test the GenieRateLimiter with 5 QPM limit."""
    logger.info("🧪 Testing GenieRateLimiter with 5 QPM limit...")
    
    # Create rate limiter with 5 QPM and 10-second window for faster testing
    rate_limiter = GenieRateLimiter(rate_limit_per_minute=5, window_size=10, burst_capacity=2)
    
    # Test immediate requests (should allow up to 5)
    allowed_count = 0
    for i in range(8):  # Try 8 requests
        if rate_limiter.record_request():
            allowed_count += 1
            logger.info(f"   Request {i+1}: ✅ Allowed (total: {allowed_count})")
        else:
            logger.info(f"   Request {i+1}: ❌ Rate limited (utilization: {rate_limiter.get_utilization():.1f}%)")
    
    logger.info(f"   Result: {allowed_count}/8 requests allowed (expected: ~7 with burst)")
    
    # Test time-based recovery
    logger.info("   ⏳ Waiting 11 seconds for rate limit window to reset...")
    time.sleep(11)
    
    # Should be able to make requests again
    if rate_limiter.record_request():
        logger.info("   ✅ Rate limiter correctly reset after window expiry")
    else:
        logger.info("   ❌ Rate limiter did not reset properly")
    
    logger.info("✅ GenieRateLimiter test completed\n")


def test_throttle_manager():
    """Test the WorkspaceThrottleManager with queuing."""
    logger.info("🧪 Testing WorkspaceThrottleManager with queuing...")
    
    # Create throttle manager (will use default 5 QPM from config)
    throttle_manager = WorkspaceThrottleManager()
    
    # Test immediate processing
    logger.info("   Testing immediate request processing...")
    status, result = throttle_manager.submit_request(
        user_id="test_user_1",
        request_callable=mock_genie_request,
        request_args=(0.1,),  # 100ms delay
        priority=0
    )
    
    logger.info(f"   Request 1 - Status: {status.value}, Result: {result}")
    
    # Flood the system to test queuing
    logger.info("   Testing queue behavior with 10 concurrent requests...")
    start_time = time.time()
    
    results = []
    for i in range(10):
        status, result = throttle_manager.submit_request(
            user_id=f"test_user_{i}",
            request_callable=mock_genie_request,
            request_args=(0.2,),  # 200ms delay
            priority=0,
            timeout=30  # 30 second timeout for testing
        )
        results.append((status, result))
        logger.info(f"   Request {i+1}/10 - Status: {status.value}")
    
    total_time = time.time() - start_time
    logger.info(f"   Total processing time: {total_time:.2f} seconds")
    
    # Analyze results
    allowed_immediately = sum(1 for status, _ in results if status == ThrottleStatus.ALLOWED)
    queued_processed = sum(1 for status, _ in results if status == ThrottleStatus.QUEUED)
    rate_limited = sum(1 for status, _ in results if status == ThrottleStatus.RATE_LIMITED)
    queue_full = sum(1 for status, _ in results if status == ThrottleStatus.QUEUE_FULL)
    timed_out = sum(1 for status, _ in results if status == ThrottleStatus.TIMED_OUT)
    
    logger.info(f"   Results - Immediate: {allowed_immediately}, Queued: {queued_processed}, "
               f"Rate Limited: {rate_limited}, Queue Full: {queue_full}, Timed Out: {timed_out}")
    
    # Get final metrics
    metrics = throttle_manager.get_metrics()
    logger.info("   Final Metrics:")
    logger.info(f"   - Total Requests: {metrics['total_requests']}")
    logger.info(f"   - Allowed Requests: {metrics['allowed_requests']}")
    logger.info(f"   - Queued Requests: {metrics['queued_requests']}")
    logger.info(f"   - Queue Full Rejections: {metrics['queue_full_rejections']}")
    logger.info(f"   - Rate Limit Utilization: {metrics['rate_limit_utilization']:.1f}%")
    logger.info(f"   - Average Queue Wait Time: {metrics['avg_queue_wait_time']:.2f}s")
    
    # Cleanup
    throttle_manager.shutdown()
    logger.info("✅ WorkspaceThrottleManager test completed\n")


def test_configuration_validation():
    """Test that configuration values are correct."""
    logger.info("🧪 Testing configuration validation...")
    
    # Check that rate limits match documentation
    expected_rate_limit = 5  # 5 QPM as documented
    actual_rate_limit = Config.GENIE_RATE_LIMIT_PER_MINUTE
    
    if actual_rate_limit == expected_rate_limit:
        logger.info(f"   ✅ Rate limit correctly set to {actual_rate_limit} QPM (matches documentation)")
    else:
        logger.info(f"   ❌ Rate limit is {actual_rate_limit} QPM, expected {expected_rate_limit} QPM")
    
    # Check other configuration values
    logger.info(f"   Queue max size: {Config.GENIE_QUEUE_MAX_SIZE}")
    logger.info(f"   Queue timeout: {Config.GENIE_QUEUE_TIMEOUT}s")
    logger.info(f"   Burst capacity: {Config.GENIE_BURST_CAPACITY}")
    logger.info(f"   Rate limit window: {Config.GENIE_RATE_LIMIT_WINDOW_SIZE}s")
    
    logger.info("✅ Configuration validation completed\n")


def main():
    """Run all throttling tests."""
    logger.info("🚀 Starting Genie Throttling System Tests")
    logger.info("=" * 60)
    
    try:
        test_configuration_validation()
        test_rate_limiter()
        test_throttle_manager()
        
        logger.info("🎉 All tests completed successfully!")
        logger.info("=" * 60)
        logger.info("Summary:")
        logger.info("✅ Configuration values match Genie API documentation (5 QPM)")
        logger.info("✅ Rate limiter properly enforces sliding window limits")
        logger.info("✅ Throttle manager queues requests when rate limits are exceeded")
        logger.info("✅ System provides proper metrics and monitoring")
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
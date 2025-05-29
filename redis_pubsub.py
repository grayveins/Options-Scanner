import redis
import json
import logging
from typing import Any, Callable, Dict, Optional
from functools import wraps
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class RedisPubSub:
    def __init__(self, host: str = "localhost", port: int = 6379, 
                 decode_responses: bool = True, max_retries: int = 3):
        """
        Initialize Redis connection with retry logic
        """
        self.host = host
        self.port = port
        self.decode_responses = decode_responses
        self.max_retries = max_retries
        self._redis_client = None
        self._connect()

    def _connect(self) -> None:
        """Establish Redis connection with retry logic"""
        for attempt in range(self.max_retries):
            try:
                self._redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    decode_responses=self.decode_responses,
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
                # Test connection
                self._redis_client.ping()
                logger.info("Successfully connected to Redis")
                return
            except redis.ConnectionError as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to connect to Redis after {self.max_retries} attempts")
                    raise
                logger.warning(f"Redis connection failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def _ensure_connection(func):
        """Decorator to ensure Redis connection is active"""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                # Test connection
                self._redis_client.ping()
            except (redis.ConnectionError, AttributeError):
                logger.warning("Redis connection lost, attempting to reconnect...")
                self._connect()
            return func(self, *args, **kwargs)
        return wrapper
    
    @_ensure_connection
    def publish_tick(self, symbol: str, data: Dict[str, Any]) -> bool:
        """
        Publish tick data to Redis channel
        Returns True if successful, False otherwise
        """
        channel = f"tick:{symbol.lower()}"  # Case-insensitive handling
        try:
            result = self._redis_client.publish(channel, json.dumps(data))
            logger.debug(f"Published to {channel} - {result} subscribers")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to publish to {channel}: {e}")
            return False
        except json.JSONEncodeError as e:
            logger.error(f"Failed to serialize data for {channel}: {e}")
            return False

    @_ensure_connection
    def subscribe_to(self, symbol: str, 
                   callback: Callable[[Dict[str, Any]], None],
                   timeout: Optional[float] = None) -> None:
        """
        Subscribe to a symbol's tick channel and process messages
        with optional timeout in seconds
        """
        channel = f"tick:{symbol.lower()}"
        pubsub = self._redis_client.pubsub()
        try:
            pubsub.subscribe(channel)
            logger.info(f"Subscribed to {channel}")
            start_time = time.time()
            for message in pubsub.listen():
                if timeout and (time.time() - start_time) > timeout:
                    logger.info(f"Subscription timeout reached for {channel}")
                    break
                if message["type"] != "message":
                    continue
                try:
                    data = json.loads(message["data"])
                    callback(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message from {channel}: {e}")
                except Exception as e:
                    logger.error(f"Error in callback for {channel}: {e}")
        except redis.RedisError as e:
            logger.error(f"Subscription error for {channel}: {e}")
        finally:
            pubsub.close()
            logger.info(f"Unsubscribed from {channel}")

    def close(self) -> None:
        """Clean up Redis connection"""
        if self._redis_client:
            self._redis_client.close()
            logger.info("Redis connection closed")


# Example usage
if __name__ == "__main__":
    def sample_callback(data: Dict[str, Any]) -> None:
        print(f"Received tick: {data['sym']} @ {data.get('p', 'N/A')}")
    try:
        pubsub = RedisPubSub()
        # Publish example
        pubsub.publish_tick("AAPL", {"sym": "AAPL", "p": 182.75, "t": int(time.time())})
        # Subscribe example (runs for 5 seconds)
        pubsub.subscribe_to("AAPL", sample_callback, timeout=5)
    finally:
        pubsub.close()

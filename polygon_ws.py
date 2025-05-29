import websocket
import json
import time
import logging
from typing import List, Dict, Any
from redis_pubsub import publish_tick
import os
from dotenv import load_dotenv
from threading import Thread
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
API_KEY = os.getenv("POLYGON_API_KEY")
if not API_KEY:
    logger.error("POLYGON_API_KEY not found in environment variables")
    sys.exit(1)

# Configuration
TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA"]  # Adjust as needed
RECONNECT_DELAY = 5  # seconds
MAX_RECONNECT_ATTEMPTS = 5
WS_URL = "wss://socket.polygon.io/stocks"

class PolygonWebSocketClient:
    def __init__(self):
        self.ws = None
        self.reconnect_attempts = 0
        self.should_reconnect = True
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def handle_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.should_reconnect = False
        if self.ws:
            self.ws.close()

    def on_message(self, ws: websocket.WebSocketApp, message: str):
        try:
            data = json.loads(message)
            if isinstance(data, list):
                for tick in data:
                    self.process_tick(tick)
            else:
                self.process_tick(data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def process_tick(self, tick: Dict[str, Any]):
        if tick.get('ev') == 'T':  # Trade tick
            try:
                symbol = tick['sym']
                publish_tick(symbol, tick)
                logger.debug(f"Published tick for {symbol}")
            except KeyError as e:
                logger.error(f"Missing expected field in tick: {e}")
            except Exception as e:
                logger.error(f"Failed to publish tick: {e}")

    def on_error(self, ws: websocket.WebSocketApp, error: Exception):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str):
        logger.info(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        if self.should_reconnect and self.reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
            self.reconnect_attempts += 1
            logger.info(f"Attempting to reconnect ({self.reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})...")
            time.sleep(RECONNECT_DELAY)
            self.run()
        else:
            logger.info("Max reconnection attempts reached or shutdown requested")

    def on_open(self, ws: websocket.WebSocketApp):
        logger.info("WebSocket connection established")
        self.reconnect_attempts = 0  # Reset on successful connection
        
        # Authenticate
        auth_msg = json.dumps({"action": "auth", "params": API_KEY})
        ws.send(auth_msg)
        
        # Subscribe to tickers
        for ticker in TICKERS:
            sub_msg = json.dumps({"action": "subscribe", "params": f"T.{ticker}"})
            ws.send(sub_msg)
            logger.debug(f"Subscribed to {ticker}")

    def run(self):
        logger.info("Starting WebSocket client")
        self.ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # Run in a separate thread to allow for graceful shutdown
        self.wst = Thread(target=self.ws.run_forever, kwargs={
            'ping_interval': 30,
            'ping_timeout': 10
        })
        self.wst.daemon = True
        self.wst.start()

def main():
    client = PolygonWebSocketClient()
    client.run()
    
    # Keep main thread alive
    try:
        while client.wst.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        client.should_reconnect = False
        if client.ws:
            client.ws.close()
        sys.exit(0)

if __name__ == "__main__":
    main()

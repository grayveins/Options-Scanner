import logging
from typing import Dict, Any
from redis_pubsub import RedisPubSub
from strategy import MomentumScanner
import signal
import sys
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class TradingBot:
    def __init__(self):
        self.base_prices: Dict[str, float] = {
            "SPY": 530.00,
            "AAPL": 185.00,
            "MSFT": 430.00,
            "NVDA": 1100.00,
            "TSLA": 185.00
        }
        self.scanner = MomentumScanner()
        self.redis_pubsub = RedisPubSub()
        self.running = False
        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        logger.info(f"Received shutdown signal {signum}")
        self.running = False

    def validate_tick(self, tick: Dict[str, Any]) -> bool:
        """Validate tick structure and content"""
        required_fields = {"sym", "p"}
        if not all(field in tick for field in required_fields):
            logger.warning(f"Invalid tick structure: {tick}")
            return False
        return True

    def handle_tick(self, tick: Dict[str, Any]):
        """Process incoming market data ticks"""
        if not self.validate_tick(tick):
            return

        symbol = tick["sym"].upper()  # Normalize symbol case
        price = tick["p"]

        try:
            if symbol == "SPY":
                self.scanner.update_spy(tick)
                return

            if base_price := self.base_prices.get(symbol):
                if self.scanner.evaluate_tick(tick, base_price):
                    logger.info(f"🔥 SIGNAL: Buy {symbol} at ${price:.2f}")
                    # Here you could add order execution logic
        except Exception as e:
            logger.error(f"Error processing tick for {symbol}: {e}", exc_info=True)

    def run(self):
        """Main execution loop"""
        self.running = True
        logger.info("Starting trading bot...")

        try:
            # Subscribe to all symbols
            for symbol in self.base_prices.keys():
                self.redis_pubsub.subscribe_to(
                    symbol,
                    self.handle_tick,
                    timeout=None  # Run indefinitely until shutdown
                )
                # Small delay between subscriptions to avoid overwhelming Redis
                time.sleep(0.1)

            # Keep the main thread alive
            while self.running:
                time.sleep(1)

        except Exception as e:
            logger.error(f"Fatal error in trading bot: {e}", exc_info=True)
        finally:
            self.cleanup()
            logger.info("Trading bot stopped")

    def cleanup(self):
        """Clean up resources"""
        self.redis_pubsub.close()
        logger.info("Cleanup completed")


if __name__ == "__main__":
    bot = TradingBot()
    bot.run()

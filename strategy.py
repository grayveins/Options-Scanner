from datetime import datetime, timezone
from typing import Dict, List, Optional
import time

class MomentumScanner:
    def __init__(self):
        self.MOMENTUM_THRESHOLD = 1.5  # % stronger than SPY
        self.VOLUME_THRESHOLD = 1000   # Minimum trade size
        self.TARGET_HOUR = 15
        self.TARGET_MINUTE_START = 45
        self.TARGET_MINUTE_END = 59
        self.HISTORY_WINDOW = 5        # Number of ticks to consider for momentum
        
        self.tick_history: Dict[str, List[dict]] = {}
        self.spy_price: Optional[float] = None
    
    def update_spy(self, tick: dict) -> None:
        """Update the current SPY price"""
        self.spy_price = tick["p"]
    
    def _is_momentum_up(self, symbol: str, current_price: float) -> bool:
        """Check if the stock shows upward momentum"""
        history = self.tick_history.get(symbol, [])
        if len(history) < self.HISTORY_WINDOW:
            return False
        
        # Get last N prices (excluding current tick)
        recent_prices = [tick["p"] for tick in history[-self.HISTORY_WINDOW:]]
        
        # Check if all recent prices are increasing
        return all(current_price > prev_price for prev_price in recent_prices)
    
    def _in_final_15_minutes(self, timestamp: int) -> bool:
        """Check if timestamp falls in the target time window"""
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        return (dt.hour == self.TARGET_HOUR and 
                self.TARGET_MINUTE_START <= dt.minute <= self.TARGET_MINUTE_END)
    
    def _calculate_relative_strength(self, price: float, base_price: float) -> float:
        """Calculate relative strength compared to SPY"""
        stock_return = (price - base_price) / base_price
        spy_return = (self.spy_price - base_price) / base_price if self.spy_price else 0
        return (stock_return - spy_return) * 100  # Return as percentage
    
    def evaluate_tick(self, tick: dict, base_price: float) -> bool:
        """Evaluate whether a tick meets all entry criteria"""
        symbol = tick["sym"]
        price = tick["p"]
        timestamp = tick["t"]
        size = tick.get("s", 0)
        
        # Store tick for momentum history (limit history size)
        self.tick_history.setdefault(symbol, []).append(tick)
        if len(self.tick_history[symbol]) > self.HISTORY_WINDOW * 2:
            self.tick_history[symbol] = self.tick_history[symbol][-self.HISTORY_WINDOW:]
        
        # Early exit checks
        if self.spy_price is None or not self._in_final_15_minutes(timestamp):
            return False
        
        rel_strength = self._calculate_relative_strength(price, base_price)
        if rel_strength < self.MOMENTUM_THRESHOLD:
            return False
        
        if size < self.VOLUME_THRESHOLD:
            return False
        
        if not self._is_momentum_up(symbol, price):
            return False
        
        print(f"[{symbol}] ✅ Entry Signal — Price: ${price:.2f}, "
              f"Size: {size}, Rel Strength: {rel_strength:.2f}%")
        return True


if __name__ == "__main__":
    scanner = MomentumScanner()
    
    # Test cases
    def run_test_case(name: str, tick: dict, base_price: float, spy_price: float, expected: bool):
        print(f"\nRunning test case: {name}")
        scanner.update_spy({"p": spy_price})
        scanner.tick_history = {}  # Reset history
        
        # Simulate history for momentum
        if "NVDA" in tick["sym"]:
            scanner.tick_history["NVDA"] = [{"p": 1130}, {"p": 1132}, {"p": 1134}, {"p": 1136}, {"p": 1138}]
        
        result = scanner.evaluate_tick(tick, base_price)
        status = "✅ PASSED" if result == expected else "❌ FAILED"
        print(f"{status} - Expected: {expected}, Got: {result}")
    
    # Current time in the target window
    now = int(time.time() * 1000)
    
    # Valid case
    run_test_case(
        "Valid entry signal",
        {"sym": "NVDA", "p": 1140.00, "t": now, "s": 1200},
        base_price=1100.00,
        spy_price=530.0,
        expected=True
    )
    
    # Volume too low
    run_test_case(
        "Volume too low",
        {"sym": "NVDA", "p": 1140.00, "t": now, "s": 500},
        base_price=1100.00,
        spy_price=530.0,
        expected=False
    )
    
    # Outside time window (1 hour earlier)
    run_test_case(
        "Outside time window",
        {"sym": "NVDA", "p": 1140.00, "t": now - 3600*1000, "s": 1200},
        base_price=1100.00,
        spy_price=530.0,
        expected=False
    )

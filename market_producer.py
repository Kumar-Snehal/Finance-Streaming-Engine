import random
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer
import market_tick_pb2

# Target configuration
BOOTSTRAP_SERVERS = ['localhost:19092']
TOPIC_NAME = 'market-ticks-proto'
TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA']

# Set up an asynchronous, high-throughput producer for binary payloads
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    linger_ms=10,        # Batch packets for 10 ms then transmit
    batch_size=32 * 1024 # 32 KB batch size
)

def on_success(record_metadata):
    pass 

def on_error(ex):
    print(f"Delivery failed: {ex}")

def stimulate_minor_price_drift(current_price):
    drift = random.normalvariate(0, 0.2)
    current_price = max(1.0, current_price + drift)
    volume = random.randint(10, 500)
    return [current_price,volume]

def generate_ticker_stream(ticker, base_price):
    current_price = base_price
    while True:

        [current_price,volume] = stimulate_minor_price_drift(current_price)

        tick = market_tick_pb2.MarketTick()
        
        tick.timestamp = datetime.now(timezone.utc).isoformat()
        tick.ticker = ticker
        tick.price = round(current_price, 2)
        tick.volume = volume
        
        payload_bytes = tick.SerializeToString()
        
        # Asynchronously send binary data to the broker
        producer.send(TOPIC_NAME, value=payload_bytes).add_callback(on_success).add_errback(on_error)
        
        # Throttle rate (~200 ticks per second per thread)
        time.sleep(0.005)

if __name__ == "__main__":
    print(f"Initializing binary Protobuf streaming to topic: {TOPIC_NAME}...")
    
    with ThreadPoolExecutor(max_workers=len(TICKERS)) as executor:
        for ticker in TICKERS:
            print(f"Starting ticker stream for: {ticker}")
            base = random.uniform(50.0, 400.0)
            executor.submit(generate_ticker_stream, ticker, base)
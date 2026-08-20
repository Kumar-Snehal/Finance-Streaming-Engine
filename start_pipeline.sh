#!/bin/bash

PROJECT_ROOT="/run/media/kumar-snehal/Personal/Snehal/College (IIT BBS)/Projects/Finance Dashboard"

echo "=========================================="
echo "🚀 Starting Financial Dashboard Pipeline"
echo "=========================================="

echo "[1/4] Starting Docker infrastructure..."
cd "$PROJECT_ROOT"
sudo docker compose up -d

cleanup() {
    echo ""
    echo "=========================================="
    echo "🛑 Shutting down pipeline components..."
    echo "=========================================="
    
    if [ ! -z "$PRODUCER_PID" ] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
        echo "   -> Stopping Python Producer (PID: $PRODUCER_PID)..."
        kill "$PRODUCER_PID"
    fi

    if [ ! -z "$FLINK_PID" ] && kill -0 "$FLINK_PID" 2>/dev/null; then
        echo "   -> Stopping Flink Engine (PID: $FLINK_PID)..."
        kill "$FLINK_PID"
    fi

    echo "[4/4] Stopping Docker containers (docker compose down)..."
    cd "$PROJECT_ROOT"
    sudo docker compose down

    echo "✅ Pipeline successfully stopped."
    exit 0
}

trap cleanup INT TERM EXIT

echo "[2/4] Launching Python Multi-Threaded Producer..."
source stream_env/bin/activate
python3 market_producer.py > producer.log 2>&1 &
PRODUCER_PID=$!
echo "   -> Producer running in background (PID: $PRODUCER_PID)"

echo "[3/4] Launching Apache Flink Processing Engine..."
cd flink-engine

export MAVEN_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"

mvn exec:java -Dexec.mainClass="com.analytics.engine.MarketStreamEngine" > flink.log 2>&1 &
FLINK_PID=$!
echo "   -> Flink engine running in background (PID: $FLINK_PID)"

echo "=========================================="
echo "✅ Pipeline is Live and Streaming!"
echo "=========================================="
echo ""
echo "🔗 Access Links:"
echo "   - Redpanda Console (Kafka UI): http://localhost:8080"
echo "   - Grafana Dashboard:           http://localhost:3000 (admin / password)"
echo "   - TimescaleDB (Postgres):      localhost:5432 (admin / password)"
echo ""
echo "📝 Logs are being piped to:"
echo "   - Producer: producer.log"
echo "   - Flink:    flink-engine/flink.log"
echo "=========================================="
echo "Press [Ctrl+C] at any time to stop everything and run docker compose down."

wait
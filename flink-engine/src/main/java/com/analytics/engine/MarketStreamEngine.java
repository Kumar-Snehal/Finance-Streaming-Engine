package com.analytics.engine;

import java.io.IOException;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import finance.MarketTickOuterClass.MarketTick;

public class MarketStreamEngine {

    public static void main(String[] args) throws Exception {
        // 1. Initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Critical for distributed systems: Checkpoint state every 10 seconds for fault tolerance
        env.enableCheckpointing(10000); 

        // 2. Configure the Redpanda / Kafka Source
        KafkaSource<MarketTick> source = KafkaSource.<MarketTick>builder()
                .setBootstrapServers("localhost:19092")
                .setTopics("market-ticks-proto")
                .setGroupId("flink-analytics-group")
                .setStartingOffsets(OffsetsInitializer.latest()) // Process only live data
                .setValueOnlyDeserializer(new ProtobufDeserializer())
                .build();

        // 3. Ingest the stream
        DataStream<MarketTick> rawStream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Redpanda-Protobuf-Source"
        );

        // 4. Stateful Window Processing (The core analytics)
        DataStream<String> vwapStream = rawStream
                .keyBy(MarketTick::getTicker) // Group data by stock ticker (AAPL, MSFT, etc.)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 10-second rolling windows
                .aggregate(new VwapAggregator()); // Apply the math

        // 5. Output the results to the console (Phase 3 will send this to TimescaleDB)
        vwapStream.print("📊 10s VWAP ->");

        // 6. Trigger Execution
        env.execute("Real-Time Financial Streaming Engine");
    }

    // --- CUSTOM DESERIALIZER ---
    // Translates the highly compressed binary back into Java objects
    public static class ProtobufDeserializer extends AbstractDeserializationSchema<MarketTick> {
        @Override
        public MarketTick deserialize(byte[] message) throws IOException {
            return MarketTick.parseFrom(message);
        }
    }

    // --- AGGREGATION LOGIC ---
    // Calculates the Volume-Weighted Average Price incrementally in memory
    public static class VwapAggregator implements AggregateFunction<MarketTick, VwapAccumulator, String> {
        
        @Override
        public VwapAccumulator createAccumulator() {
            return new VwapAccumulator();
        }

        @Override
        public VwapAccumulator add(MarketTick tick, VwapAccumulator acc) {
            acc.ticker = tick.getTicker();
            acc.sumPriceVolume += (tick.getPrice() * tick.getVolume());
            acc.sumVolume += tick.getVolume();
            acc.tradeCount += 1;
            return acc;
        }

        @Override
        public String getResult(VwapAccumulator acc) {
            double vwap = acc.sumPriceVolume / acc.sumVolume;
            return String.format("[%s] VWAP: $%.2f | Total Volume: %d | Trades Processed: %d", 
                    acc.ticker, vwap, acc.sumVolume, acc.tradeCount);
        }

        @Override
        public VwapAccumulator merge(VwapAccumulator a, VwapAccumulator b) {
            a.sumPriceVolume += b.sumPriceVolume;
            a.sumVolume += b.sumVolume;
            a.tradeCount += b.tradeCount;
            return a;
        }
    }

    // A simple object to hold running totals in memory during the 10-second window
    public static class VwapAccumulator {
        public String ticker = "";
        public double sumPriceVolume = 0.0;
        public long sumVolume = 0;
        public long tradeCount = 0;
    }
}
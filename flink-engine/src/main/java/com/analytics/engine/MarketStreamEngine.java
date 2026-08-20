package com.analytics.engine;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import finance.MarketTickOuterClass.MarketTick;

public class MarketStreamEngine {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000); 

        KafkaSource<MarketTick> source = KafkaSource.<MarketTick>builder()
                .setBootstrapServers("localhost:19092")
                .setTopics("market-ticks-proto")
                .setGroupId("flink-analytics-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new ProtobufDeserializer())
                .build();

        DataStream<MarketTick> rawStream = env.fromSource(
                source, 
                WatermarkStrategy.noWatermarks(), 
                "Redpanda-Protobuf-Source"
        );

        // Process windows and emit structured VwapRecord objects
        DataStream<VwapRecord> vwapStream = rawStream
                .keyBy(MarketTick::getTicker)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new VwapAggregator(), new WindowResultFunction());

        // Print to console for real-time visibility
        vwapStream.map(record -> String.format("📊 DB Sink -> [%s] Time: %s | VWAP: $%.2f | Vol: %d", 
                record.ticker, record.time, record.vwap, record.totalVolume)).print();

        // Sink aggregated metrics straight into TimescaleDB
        vwapStream.addSink(JdbcSink.sink(
                "INSERT INTO stock_vwap (time, ticker, vwap, total_volume, trade_count) VALUES (?, ?, ?, ?, ?)",
                (JdbcStatementBuilder<VwapRecord>) (ps, record) -> {
                    ps.setTimestamp(1, Timestamp.from(record.time));
                    ps.setString(2, record.ticker);
                    ps.setDouble(3, record.vwap);
                    ps.setLong(4, record.totalVolume);
                    ps.setLong(5, record.tradeCount);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://localhost:5432/finance")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("admin")
                        .withPassword("password")
                        .build()
        ));

        env.execute("Real-Time Financial Streaming Engine with TimescaleDB Sink");
    }

    public static class ProtobufDeserializer extends AbstractDeserializationSchema<MarketTick> {
        @Override
        public MarketTick deserialize(byte[] message) throws IOException {
            return MarketTick.parseFrom(message);
        }
    }

    public static class VwapAggregator implements AggregateFunction<MarketTick, VwapAccumulator, VwapAccumulator> {
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
        public VwapAccumulator getResult(VwapAccumulator acc) {
            return acc;
        }

        @Override
        public VwapAccumulator merge(VwapAccumulator a, VwapAccumulator b) {
            a.sumPriceVolume += b.sumPriceVolume;
            a.sumVolume += b.sumVolume;
            a.tradeCount += b.tradeCount;
            return a;
        }
    }

    // Extracts the window end time and packages the final metrics
    public static class WindowResultFunction extends ProcessWindowFunction<VwapAccumulator, VwapRecord, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<VwapAccumulator> elements, Collector<VwapRecord> out) {
            VwapAccumulator acc = elements.iterator().next();
            double vwap = acc.sumVolume == 0 ? 0.0 : acc.sumPriceVolume / acc.sumVolume;
            Instant windowEnd = Instant.ofEpochMilli(context.window().getEnd());

            out.collect(new VwapRecord(
                windowEnd,
                acc.ticker,
                vwap,
                acc.sumVolume,
                acc.tradeCount
            ));
        }
    }

    public static class VwapAccumulator {
        public String ticker = "";
        public double sumPriceVolume = 0.0;
        public long sumVolume = 0;
        public long tradeCount = 0;
    }

    // Immutable POJO for database persistence
    public static class VwapRecord {
        public Instant time;
        public String ticker;
        public double vwap;
        public long totalVolume;
        public long tradeCount;

        public VwapRecord() {}
        public VwapRecord(Instant time, String ticker, double vwap, long totalVolume, long tradeCount) {
            this.time = time;
            this.ticker = ticker;
            this.vwap = vwap;
            this.totalVolume = totalVolume;
            this.tradeCount = tradeCount;
        }
    }
}
package com.jdriven.kafkaworkshop;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;

import static com.jdriven.kafkaworkshop.TopicNames.AVERAGE_TEMPS;
import static com.jdriven.kafkaworkshop.TopicNames.RECEIVED_SENSOR_DATA;
import static java.lang.String.format;
import static java.lang.System.out;
import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.common.serialization.Serdes.String;

@Component
public class KafkaStreamsRunner implements CommandLineRunner {

    final StreamsConfig streamsConfig;
    final StreamsBuilder streamsBuilder;

    public KafkaStreamsRunner(final StreamsConfig streamsConfig, final StreamsBuilder streamsBuilder) {
        this.streamsConfig = streamsConfig;
        this.streamsBuilder = streamsBuilder;
    }

    @Override
    public void run(final String... args) {
        streamsBuilder.stream(RECEIVED_SENSOR_DATA, Consumed.with(String(), new JsonSerde<>(SensorData.class)))
            //    .peek((k, sensorData) -> out.println("Received on 'received-sensor-data': " + sensorData))
                .groupByKey()
                .windowedBy(TimeWindows.of(30000).advanceBy(10000))
                .aggregate(SumCount::new, (key, value, aggregate) -> aggregate.addValue(value.getTemperature()), Materialized.with(String(), new JsonSerde<>(SumCount.class)))
                .mapValues(SumCount::average, Materialized.with(new WindowedSerde<>(String()), Double()))
                .toStream()
            //    .peek((key, average) -> out.println(format("Produced average %.2f for id %s in window %s", average, key.key(), key.window())))
                .map(((key, average) -> new KeyValue<>(key.key(), new Average(average, key.window().start(), key.window().start() + 30000))))
                .through(TopicNames.AVERAGE_TEMPS, Produced.with(String(), new JsonSerde<>(Average.class)))

                .foreach((key, average) -> out.println(format("Received average %s for id %s on %s", average, key, AVERAGE_TEMPS)));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    static class WindowedSerde<T> implements Serde<Windowed<T>> {

        private final Serde<Windowed<T>> inner;

        public WindowedSerde(Serde<T> serde) {
            inner = Serdes.serdeFrom(
                    new WindowedSerializer<>(serde.serializer()),
                    new WindowedDeserializer<>(serde.deserializer()));
        }

        @Override
        public Serializer<Windowed<T>> serializer() {
            return inner.serializer();
        }

        @Override
        public Deserializer<Windowed<T>> deserializer() {
            return inner.deserializer();
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            inner.serializer().configure(configs, isKey);
            inner.deserializer().configure(configs, isKey);
        }

        @Override
        public void close() {
            inner.serializer().close();
            inner.deserializer().close();
        }

    }

    static class Average {
        double average;
        long start;
        long end;

        public Average(final double average, final long start, final long end) {
            this.average = average;
            this.start = start;
            this.end = end;
        }

        public Average() {
        }

        public double getAverage() {
            return average;
        }

        public void setAverage(final double average) {
            this.average = average;
        }

        public long getStart() {
            return start;
        }

        public void setStart(final long start) {
            this.start = start;
        }

        public long getEnd() {
            return end;
        }

        public void setEnd(final long end) {
            this.end = end;
        }

        @Override
        public String toString() {
            return format("%.2f for %2$tH:%2$tM:%2$tS-%3$tH:%3$tM:%3$tS", average, new Date(start), new Date(end));
        }
    }

    static class SumCount {
        double sum;
        int count;

        public SumCount() {
        }

        public SumCount addValue(double value) {
            sum += value;
            count++;
            return this;
        }

        public double average() {
            return sum / count;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(final double sum) {
            this.sum = sum;
        }

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }
}

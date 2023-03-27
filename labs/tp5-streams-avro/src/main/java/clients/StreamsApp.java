package clients;

import clients.avro.PositionString;
import clients.avro.PositionValue;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamsApp {

    private static final Logger logger = LoggerFactory.getLogger(StreamsApp.class);

    /**
     * Our first streams app.
     */
    public static void main(String[] args) {
        logger.info(">>> Starting the streams-app Application");

        final Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-app-1");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName()
        );
        settings.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        final Topology topology = getTopology();
        // you can paste the topology into this site for a vizualization: https://zz85.github.io/kafka-streams-viz/
        logger.info(topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, settings);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("<<< Stopping the streams-app Application");
            streams.close();
            latch.countDown();
        }));

        // don't do this in prod as it clears your state stores
        streams.cleanUp();
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Topology getTopology() {

        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");
        final Serde<PositionValue> positionValueSerde = new SpecificAvroSerde<>();
        positionValueSerde.configure(serdeConfig, false);
        final Serde<PositionString> positionStringSerde = new SpecificAvroSerde<>();
        positionStringSerde.configure(serdeConfig, false);

        // Create the StreamsBuilder object to create our Topology
        final StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the `driver-positions-avro` topic
        // configure a serdes that can read the string key, and avro value
        final KStream<String, PositionValue> positions = builder.stream(
            "driver-positions-kstreams-avro",
            Consumed.with(
                Serdes.String(),
                positionValueSerde
            )
        );

        // TODO: Use filter() method to filter out the events from `driver-2`.
        final KStream<String, PositionValue> positionsFiltered = positions.filter(???);

        // TODO: Use mapValues() method to change the value of each event from PositionValue to PositionString class.
        //       You can check the two schemas under src/main/avro/. Notice that position_string.avsc contains a new field
        //       `positionString` as String type.
        final KStream<String, PositionString> positionsString = positionsFiltered.mapValues(???);

        // TODO: Use peek() to print for each record, the value of the new field `positionString`
        final KStream<String, PositionString> positionPrinted = positionsString.peek(???);

        // Write the results to topic `driver-positions-string-avro`
        // configure a serdes that can write the string key, and new avro value
        positionPrinted.to("driver-positions-string-avro", Produced.with(Serdes.String(), positionStringSerde));

        // Build the Topology
        final Topology topology = builder.build();
        return topology;
    }

}

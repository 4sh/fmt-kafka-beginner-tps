package clients;

import clients.avro.PositionValue;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class Producer {

    static final String DRIVER_FILE_PREFIX = "resources/drivers/";
    static final String KAFKA_TOPIC = "driver-positions-avro";

    /**
     * Java producer.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Starting Java Avro producer.");

        // Load a driver id from an environment variable
        // if it isn't present use "driver-1"
        String driverId = System.getenv("DRIVER_ID");
        driverId = (driverId != null) ? driverId : "driver-1";

        // Configure the location of the bootstrap server, default serializers,
        // Confluent interceptors, schema registry location
        final Properties settings = new Properties();
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, driverId);
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        settings.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        settings.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, List.of(MonitoringProducerInterceptor.class));

        final KafkaProducer<String, PositionValue> producer = new KafkaProducer<>(settings);

        // Adding a shutdown hook to clean up when the application exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Closing producer.");
            producer.close();
        }));

        int pos = 0;
        final String[] rows = Files.readAllLines(Paths.get(DRIVER_FILE_PREFIX + driverId + ".csv"), StandardCharsets.UTF_8)
            .toArray(new String[0]);

        // Loop forever over the driver CSV file..
        while (true) {
            final String key = driverId;
            final Double latitude1 = Double.parseDouble(rows[pos].split(",")[0]);
            final Double longitude1 = Double.parseDouble(rows[pos].split(",")[1]);
            final PositionValue value = new PositionValue(latitude1, longitude1);
            final ProducerRecord<String, PositionValue> record = new ProducerRecord<>(
                KAFKA_TOPIC, key, value);
            producer.send(record, (md, e) -> {
                if (null != e) {
                    System.out.printf(
                        "Sent Key:%s Latitude:%s Longitude:%s\n => error : %s",
                        key,
                        value.getLatitude(),
                        value.getLongitude(),
                        e.getMessage()
                    );
                } else {
                    System.out.printf(
                        "Sent Key:%s Latitude:%s Longitude:%s\n",
                        key,
                        value.getLatitude(),
                        value.getLongitude()
                    );
                }
            });
            Thread.sleep(1000);
            pos = (pos + 1) % rows.length;
        }
    }
}

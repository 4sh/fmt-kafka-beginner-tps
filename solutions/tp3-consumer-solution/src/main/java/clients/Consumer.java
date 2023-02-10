package clients;

import io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

    static final String KAFKA_TOPIC = "driver-positions";

    /**
     * Java consumer.
     */
    public static void main(String[] args) {
        System.out.println("Starting Java Consumer.");

        // Configure the group id, location of the bootstrap server, default deserializers,
        // Confluent interceptors
        final Properties settings = new Properties();
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer");
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        settings.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, List.of(MonitoringConsumerInterceptor.class));

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings);

        try {
            // Subscribe to our topic
            consumer.subscribe(List.of(KAFKA_TOPIC));
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                        "Key:%s Value:%s [partition %s][offset %s]\n",
                        record.key(),
                        record.value(),
                        record.partition(),
                        record.offset()
                    );
                }
            }
        } finally {
            // Clean up when the application exits or errors
            System.out.println("Closing consumer.");
            consumer.close();
        }
    }
}

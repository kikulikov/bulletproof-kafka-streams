package io.confluent.main;

import io.confluent.bulletproof.ThreadLocalContextContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.streams.FailingUppercaseTopology.INPUT_TOPIC;
import static io.confluent.streams.FailingUppercaseTopology.OUTPUT_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

class FailingUppercaseTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.0.1");
    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();
    private static final String APPLICATION_ID = "TEST";

    @ClassRule
    static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);
    private KafkaStreams kafkaStreams;

    @BeforeEach
    void setUp() throws Exception {
        kafka.withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "1");
        kafka.withStartupTimeout(Duration.ofSeconds(60));
        kafka.start();

        try (var admin = AdminClient.create(adminConfig())) {
            admin.createTopics(Stream.of(INPUT_TOPIC, OUTPUT_TOPIC)
                    .map(n -> new NewTopic(n, 3, (short) 1))
                    .collect(Collectors.toList())).all().get();
        }

        kafkaStreams = FailingUppercase.kafkaStreamsWithHandler(streamsConfig());
        kafkaStreams.start();
    }

    private Properties adminConfig() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-admin");
        return props;
    }

    private Properties producerConfig(Class<? extends Serializer<?>> valueKlass) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueKlass.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-producer");
        return props;
    }

    private Properties consumerConfig(Class<? extends Deserializer<?>> valueKlass) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueKlass.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID + "-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private Properties streamsConfig() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        return props;
    }

    @Test
    void manipulatingContext() throws Exception {

        new Thread("another thread without context") {
            public void run() {
                assertThat(ThreadLocalContextContainer.threadLocal.get()).isNull();
            }
        }.start();

        Awaitility.pollInSameThread();

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500))
                .until(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING);

        try (var producer = new KafkaProducer<String, String>(producerConfig(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "moo")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "meow")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "quack")).get(); // failure
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "oink")).get();
        }

        final var results = new ArrayList<ConsumerRecord<String, String>>();

        try (var consumer = new KafkaConsumer<String, String>(consumerConfig(StringDeserializer.class))) {
            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));

            for (int i = 0; i < 10; i++) {
                consumer.poll(Duration.ofMillis(200)).forEach(results::add);
            }
        }

        final var strings = results.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        assertThat(strings).containsOnly("MOO", "MEOW", "OINK");
        assertThat(strings.size()).isEqualTo(3);
    }

    @AfterEach
    void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        kafka.close();
    }
}
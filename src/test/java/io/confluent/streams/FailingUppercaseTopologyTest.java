package io.confluent.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.confluent.streams.FailingUppercaseTopology.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FailingUppercaseTopologyTest {

    public static final Serde<String> STRING_SERDE = Serdes.String();
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setUp() {
        final Topology topology = FailingUppercaseTopology.buildTopology();
        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, getConfig());

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC,
                STRING_SERDE.serializer(), STRING_SERDE.serializer());

        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC,
                STRING_SERDE.deserializer(), STRING_SERDE.deserializer());
    }

    private Properties getConfig() {
        final String stringSerde = STRING_SERDE.getClass().getName();

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "UNIT-TEST");
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde);
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde);

        return config;
    }

    @Test
    void testBasicScenario() {
        inputTopic.pipeInput("moo");
        Assertions.assertThat(outputTopic.readValue()).isEqualTo("MOO");

        inputTopic.pipeInput("oink");
        Assertions.assertThat(outputTopic.readValue()).isEqualTo("OINK");
    }

    @Test
    void testFailingScenario() {
        assertThrows(RuntimeException.class, () -> inputTopic.pipeInput(STOPWORD));
    }
}
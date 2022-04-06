package io.confluent.main;

import io.confluent.bulletproof.ReplaceThreadExceptionHandler;
import io.confluent.streams.FailingUppercaseTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FailingUppercase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailingUppercase.class);
    private static final String STRING_SERDE_CLASS = Serdes.String().getClass().getName();

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage <streamsPropertiesFile>");
            System.exit(1);
        }

        final Properties streamsConfig = loadConfig(args[0]);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_CLASS);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_CLASS);

        LOGGER.info("Starting the application..");
        final KafkaStreams streams = kafkaStreamsWithHandler(streamsConfig);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await(); // awaits for the countdown
            streams.close();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }

    static KafkaStreams kafkaStreamsWithHandler(Properties streamsConfig) {
        final Topology topology = FailingUppercaseTopology.buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

        // the special exception handler committing the offset regarding the failure and replacing the thread
        streams.setUncaughtExceptionHandler(new ReplaceThreadExceptionHandler());

        return streams;
    }

    private static Properties loadConfig(final String configFile) throws IOException {
        final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        final Properties cfg = new Properties();

        try (InputStream inputStream = classloader.getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        }

        return cfg;
    }
}

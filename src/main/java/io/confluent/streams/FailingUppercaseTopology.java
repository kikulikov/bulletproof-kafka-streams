package io.confluent.streams;

import io.confluent.bulletproof.ExtractContextProcessor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Locale;

public class FailingUppercaseTopology {

    public static final String INPUT_TOPIC = "uppercase-input";
    public static final String OUTPUT_TOPIC = "uppercase-output";
    public static final String STOPWORD = "quack";

    public static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // using the special processor for extracting the context and persisting it with ThreadLocal
        inputStream.process(() -> new ExtractContextProcessor<>());

        // stream processing where RuntimeException is thrown when STOPWORD has been met
        inputStream.mapValues(FailingUppercaseTopology::uppingOrThrowing).to(OUTPUT_TOPIC);

        return builder.build();
    }

    private static String uppingOrThrowing(String s) {
        if (STOPWORD.equals(s)) throw new RuntimeException("quacking failure"); // might fail here
        return s.toUpperCase(Locale.ROOT);
    }
}

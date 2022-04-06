package io.confluent.bulletproof;

import org.apache.kafka.streams.processor.api.ProcessorContext;

public class ThreadLocalContextContainer {
    public static final ThreadLocal<ProcessorContext> threadLocal = new ThreadLocal<>();
}

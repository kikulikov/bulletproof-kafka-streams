package io.confluent.bulletproof;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ExtractContextProcessor<K, V> implements Processor<K, V, Void, Void> {

    @Override
    public void process(Record<K, V> record) {
        // dead end, do nothing here
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        ThreadLocalContextContainer.threadLocal.set(context);
    }

    @Override
    public void close() {
        ThreadLocalContextContainer.threadLocal.remove();
    }
}
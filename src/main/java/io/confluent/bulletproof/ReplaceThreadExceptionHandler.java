package io.confluent.bulletproof;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceThreadExceptionHandler implements StreamsUncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceThreadExceptionHandler.class);

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        final var context = ThreadLocalContextContainer.threadLocal.get();

        LOGGER.warn("Handling the failure and promoting the offset for " + context.applicationId());
        context.commit(); // committing the current offset though it was not processed properly

        return StreamThreadExceptionResponse.REPLACE_THREAD; // replaces the thread to proceed
    }
}

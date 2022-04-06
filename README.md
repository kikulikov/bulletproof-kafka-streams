# Bulletproof Kafka Streams

`GIVEN` A Kafka Streams application processing events from various network IoT devices 
`WHEN` a corrupted message causes a runtime exception during the processing phase
`THEN` we want to skip the corrupted message and carry on processing without any human intervention

https://developer.confluent.io/learn-kafka/kafka-streams/error-handling/ 

Kafka Streams has three broad categories of errors: entry (serialization) errors, 
processing (user logic) errors, and exit (producer) errors.

Entry errors handler has a default configuration of `LogAndFailExceptionHandler`. 
This default handler logs the exception and then fails. The other option is the 
`LogAndContinueExceptionHandler`, which logs the error but continues to run.

Exit errors are handled with the `ProductionExceptionHandler` interface, 
and you can respond by continuing to process, or by failing.

We have good control over the entry and exit phases where we can configure
the application to ignore the error and carry on. But there are no such mechanisms 
for the processing phase, as we can only `Replace the Thread` or `Shutdown the App`. 

The idea is to preserve Kafka Streams `ProcessorContext` with `ThreadLocal` which allows each 
thread to hold an implicit reference to the copy of a thread-local variable as long as the thread is alive. 
To preserve the context a special processor is used `ExtractContextProcessor`. Finally, 
the exception handler `ReplaceThreadExceptionHandler` is able to get the context 
from `ThreadLocalContextContainer`, commit the offset and replace the failed thread. 
So, the newly created thread can continue where the previous one had failed skipping 
the corrupted event and carrying on processing.


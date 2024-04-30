package org.uniprot.store.spark.indexer.common.writer;

import java.time.Duration;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import voldemort.VoldemortException;

/**
 * This class is responsible to write an RDD partition (Entry Iterator) into our DataStore
 *
 * @author lgonzales
 * @since 30/07/2020
 */
@Slf4j
public abstract class AbstractDataStoreWriter<T> implements VoidFunction<Iterator<T>> {

    private static final long serialVersionUID = -7494935529238007874L;
    protected final DataStoreParameter parameter;

    public AbstractDataStoreWriter(DataStoreParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void call(Iterator<T> entryIterator) throws Exception {
        try (VoldemortClient<T> client = getDataStoreClient()) {
            RetryPolicy<Object> retryPolicy = getVoldemortRetryPolicy();
            while (entryIterator.hasNext()) {
                final T entry = entryIterator.next();
                Failsafe.with(retryPolicy).run(() -> client.saveEntry(entry));
            }
        }
    }

    protected abstract VoldemortClient<T> getDataStoreClient();

    private RetryPolicy<Object> getVoldemortRetryPolicy() {
        return new RetryPolicy<>()
                .handle(VoldemortException.class)
                .withDelay(Duration.ofMillis(parameter.getDelay()))
                .onFailedAttempt(e -> log.warn("Voldemort save attempt failed"))
                .withMaxRetries(parameter.getMaxRetry());
    }
}

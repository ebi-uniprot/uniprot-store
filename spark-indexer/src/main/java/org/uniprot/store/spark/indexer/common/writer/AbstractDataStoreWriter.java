package org.uniprot.store.spark.indexer.common.writer;

import java.time.Duration;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

import voldemort.VoldemortException;

/**
 * This class is responsible to write an RDD partition (Entry Iterator) into our DataStore
 *
 * @author lgonzales
 * @since 30/07/2020
 */
@Slf4j
public abstract class AbstractDataStoreWriter<T> implements VoidFunction<Iterator<T>> {

    private static final long serialVersionUID = -6006659302869170050L;
    protected final DataStoreParameter parameter;
    private final RetryPolicy<Object> retryPolicy;

    public AbstractDataStoreWriter(DataStoreParameter parameter) {
        this.parameter = parameter;
        this.retryPolicy =
                new RetryPolicy<>()
                        .handle(VoldemortException.class)
                        .withDelay(Duration.ofMillis(parameter.getDelay()))
                        .onFailedAttempt(e -> log.warn("voldemort save attempt failed"))
                        .withMaxRetries(parameter.getMaxRetry());
    }

    @Override
    public void call(Iterator<T> entryIterator) throws Exception {
        VoldemortClient<T> client = getDataStoreClient();
        while (entryIterator.hasNext()) {
            final T entry = entryIterator.next();
            Failsafe.with(retryPolicy).run(() -> client.saveEntry(entry));
        }
    }

    protected abstract VoldemortClient<T> getDataStoreClient();
}

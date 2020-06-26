package org.uniprot.store.spark.indexer.common.writer;

import java.time.Duration;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.store.datastore.voldemort.VoldemortClient;

import voldemort.VoldemortException;

/**
 * @author lgonzales
 * @since 26/04/2020
 */
@Slf4j
public class DataStoreWriter<T> {

    private final VoldemortClient<T> client;
    private final RetryPolicy<Object> retryPolicy;

    public DataStoreWriter(VoldemortClient<T> client) {
        this.client = client;
        this.retryPolicy =
                new RetryPolicy<>()
                        .handle(VoldemortException.class)
                        .withDelay(Duration.ofMillis(4000))
                        .onFailedAttempt(e -> log.warn("voldemort save attempt failed"))
                        .withMaxRetries(3);
    }

    public void indexInStore(Iterator<T> uniProtEntryIterator) {
        while (uniProtEntryIterator.hasNext()) {
            final T entry = uniProtEntryIterator.next();
            Failsafe.with(retryPolicy).run(() -> client.saveEntry(entry));
        }
    }
}

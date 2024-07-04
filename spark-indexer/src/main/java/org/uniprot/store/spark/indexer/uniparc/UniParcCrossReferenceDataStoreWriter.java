package org.uniprot.store.spark.indexer.uniparc;

import java.io.Serial;
import java.time.Duration;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import voldemort.VoldemortException;

@Slf4j
public class UniParcCrossReferenceDataStoreWriter
        implements VoidFunction<Iterator<UniParcCrossReferenceWrapper>> {

    @Serial private static final long serialVersionUID = -8728253356930115287L;
    private final DataStoreParameter parameter;

    public UniParcCrossReferenceDataStoreWriter(DataStoreParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void call(Iterator<UniParcCrossReferenceWrapper> entryIterator) throws Exception {
        try (VoldemortClient<UniParcCrossReference> client = getDataStoreClient()) {
            RetryPolicy<Object> retryPolicy = getVoldemortRetryPolicy();
            while (entryIterator.hasNext()) {
                final UniParcCrossReferenceWrapper entry = entryIterator.next();
                Failsafe.with(retryPolicy)
                        .run(
                                () ->
                                        client.saveEntry(
                                                entry.getId(), entry.getUniParcCrossReference()));
            }
        }
    }

    VoldemortClient<UniParcCrossReference> getDataStoreClient() {
        return new VoldemortRemoteUniParcCrossReferenceStore(
                parameter.getNumberOfConnections(),
                parameter.isBrotliEnabled(),
                parameter.getBrotliLevel(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }

    private RetryPolicy<Object> getVoldemortRetryPolicy() {
        return new RetryPolicy<>()
                .handle(VoldemortException.class)
                .withDelay(Duration.ofMillis(parameter.getDelay()))
                .onFailedAttempt(e -> log.warn("Voldemort save attempt failed"))
                .withMaxRetries(parameter.getMaxRetry());
    }
}

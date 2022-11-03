package org.uniprot.store.spark.indexer.common.writer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

/**
 * @author lgonzales
 * @since 03/09/2020
 */
@Slf4j
public class SolrIndexWriter implements VoidFunction<Iterator<SolrInputDocument>> {

    private static final long serialVersionUID = -4229642171927549015L;
    protected final SolrIndexParameter parameter;

    public SolrIndexWriter(SolrIndexParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void call(Iterator<SolrInputDocument> docs) throws Exception {
        RetryPolicy<Object> retryPolicy = getSolrRetryPolicy();
        try (SolrClient client = getSolrClient()) {
            BatchIterable iterable = new BatchIterable(docs, parameter.getBatchSize());
            for (Collection<SolrInputDocument> batch : iterable) {
                Failsafe.with(retryPolicy)
                        .onFailure(
                                throwable ->
                                        log.error(
                                                "Failed to write to Solr", throwable.getFailure()))
                        .run(() -> client.add(parameter.getCollectionName(), batch));
            }
        } catch (Exception e) {
            String errorMessage =
                    "Exception indexing data to Solr, for collection "
                            + parameter.getCollectionName();
            throw new SolrIndexException(errorMessage, e);
        }
    }

    protected SolrClient getSolrClient() {
        return new CloudSolrClient.Builder(singletonList(parameter.getZkHost()), Optional.empty())
                .build();
    }

    private RetryPolicy<Object> getSolrRetryPolicy() {
        return new RetryPolicy<>()
                .handle(
                        asList(
                                HttpSolrClient.RemoteSolrException.class,
                                SolrServerException.class,
                                SolrException.class))
                .withDelay(Duration.ofMillis(parameter.getDelay()))
                .onFailedAttempt(e -> log.warn("Solr save attempt failed", e.getLastFailure()))
                .withMaxRetries(parameter.getMaxRetry());
    }

    private static class BatchIterable implements Iterable<Collection<SolrInputDocument>> {
        private final Iterator<SolrInputDocument> sourceIterator;
        private final int batchSize;

        public BatchIterable(Iterator<SolrInputDocument> sourceIterator, int batchSize) {
            if(batchSize <= 0){
                throw new IllegalArgumentException("Batch size must be bigger than 1. Current value is:"+batchSize);
            }
            this.batchSize = batchSize;
            this.sourceIterator = sourceIterator;
        }

        @Override
        public Iterator<Collection<SolrInputDocument>> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return sourceIterator.hasNext();
                }

                @Override
                public List<SolrInputDocument> next() {
                    List<SolrInputDocument> batch = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        if (sourceIterator.hasNext()) {
                            batch.add(sourceIterator.next());
                        } else {
                            break;
                        }
                    }
                    return batch;
                }
            };
        }
    }
}

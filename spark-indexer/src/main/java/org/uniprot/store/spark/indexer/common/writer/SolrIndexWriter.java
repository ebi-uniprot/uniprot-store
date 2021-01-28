package org.uniprot.store.spark.indexer.common.writer;

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

import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * @author lgonzales
 * @since 03/09/2020
 */
@Slf4j
public class SolrIndexWriter implements VoidFunction<Iterator<SolrInputDocument>> {

    private static final long serialVersionUID = 1997175675889081522L;
    protected final SolrIndexParameter parameter;

    public SolrIndexWriter(SolrIndexParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void call(Iterator<SolrInputDocument> docs) throws Exception {
        RetryPolicy<Object> retryPolicy =
                new RetryPolicy<>()
                        .handle(
                                asList(
                                        HttpSolrClient.RemoteSolrException.class,
                                        SolrServerException.class,
                                        SolrException.class))
                        .withDelay(Duration.ofMillis(parameter.getDelay()))
                        .onFailedAttempt(
                                e -> log.warn("Solr save attempt failed", e.getLastFailure()))
                        .withMaxRetries(parameter.getMaxRetry());

        try (SolrClient client = getSolrClient()) {
            while (docs.hasNext()) {
                SolrInputDocument doc = docs.next();
                Failsafe.with(retryPolicy)
                        .onFailure(
                                throwable ->
                                        log.error(
                                                "Failed to write to Solr", throwable.getFailure()))
                        .run(() -> client.add(parameter.getCollectionName(), doc));
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
}

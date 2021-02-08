package org.uniprot.store.spark.indexer.common.writer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import org.uniprot.core.util.Utils;
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
            Iterable<SolrInputDocument> docsIterable = () -> docs;
            List<SolrInputDocument> docList =
                    StreamSupport.stream(docsIterable.spliterator(), false)
                            .collect(Collectors.toList());
            if (Utils.notNullNotEmpty(docList)) {
                Failsafe.with(retryPolicy)
                        .onFailure(
                                throwable ->
                                        log.error(
                                                "Failed to write to Solr", throwable.getFailure()))
                        .run(() -> client.add(parameter.getCollectionName(), docList));
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
}

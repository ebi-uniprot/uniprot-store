package org.uniprot.store.indexer.common.config;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;

/**
 * A wrapper of {@link SolrClient} which creates a {@link SolrClient} instance for each thread that
 * it is used on. The purpose of this is to ensure that multi-threaded applications have multiple
 * instances that can access Solr, e.g., improving writing throughput.
 *
 * <p>Created 10/07/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtSolrClient {
    private final ThreadLocal<SolrClient> threadLocalSolrClients;
    private final RepositoryConfigProperties config;

    public UniProtSolrClient(RepositoryConfigProperties config) {
        this.config = config;
        this.threadLocalSolrClients = ThreadLocal.withInitial(this::uniProtSolrClient);
    }

    public UniProtSolrClient(SolrClient solrOperations) {
        this.config = null;
        this.threadLocalSolrClients = ThreadLocal.withInitial(() -> solrOperations);
    }

    public <T extends Document> List<T> query(
            SolrCollection collection, SolrQuery query, Class<T> documentClass) {
        String collectionString = collection.name();
        try {
            QueryResponse response = threadLocalSolrClients.get().query(collectionString, query);
            return response.getBeans(documentClass);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not query from Solr collection [" + collectionString + "]", e);
        }
    }

    public QueryResponse query(SolrCollection collection, SolrQuery query) {
        String collectionString = collection.name();
        try {
            return threadLocalSolrClients.get().query(collectionString, query);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not query from Solr collection [" + collectionString + "]", e);
        }
    }

    public UpdateResponse saveBeans(SolrCollection collection, Collection<?> beans) {
        String collectionString = collection.name();
        try {
            return threadLocalSolrClients.get().addBeans(collectionString, beans);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not write documents to Solr collection [" + collectionString + "]", e);
        }
    }

    public <T> Optional<T> queryForObject(
            SolrCollection collection, SolrQuery solrQuery, Class<T> returnType) {
        String collectionString = collection.name();
        try {
            QueryResponse response =
                    threadLocalSolrClients.get().query(collectionString, solrQuery);
            List<T> beans = response.getBeans(returnType);
            if (beans.size() == 1) {
                return Optional.of(beans.get(0));
            } else {
                return Optional.empty();
            }
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Query to collection '"
                            + collectionString
                            + "' returned multiple documents, but only one was expected",
                    e);
        }
    }

    public void commit(SolrCollection collection) {
        String collectionString = collection.name();
        try {
            threadLocalSolrClients.get().commit(collectionString);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not commit contents of Solr collection [" + collectionString + "]", e);
        }
    }

    public void softCommit(SolrCollection collection) {
        String collectionString = collection.name();
        try {
            threadLocalSolrClients.get().commit(collectionString, true, true, true);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not soft commit contents of Solr collection [" + collectionString + "]",
                    e);
        }
    }

    public void delete(SolrCollection collection, String query) {
        String collectionString = collection.name();
        try {
            threadLocalSolrClients.get().deleteByQuery(collectionString, query);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryRetrievalException(
                    "Could not delete by query for Solr collection [" + collectionString + "]", e);
        }
    }

    public void cleanUp() {
        threadLocalSolrClients.remove();
    }

    private SolrClient uniProtSolrClient() {
        String zookeeperhost = config.getZkHost();
        if (zookeeperhost != null && !zookeeperhost.isEmpty()) {
            String[] zookeeperHosts = zookeeperhost.split(",");

            CloudSolrClient client =
                    new CloudSolrClient.Builder(asList(zookeeperHosts), Optional.empty())
                            .withConnectionTimeout(config.getConnectionTimeout())
                            .withSocketTimeout(config.getSocketTimeout())
                            .build();

            client.setIdField(
                    "accession_id"); // TODO: 10/07/19 refactor schemas to include unique field 'id'
            return client;
        } else if (!config.getHttphost().isEmpty()) {
            return new HttpSolrClient.Builder()
                    .withHttpClient(httpClient())
                    .withBaseSolrUrl(config.getHttphost())
                    .build();
        } else {
            throw new IllegalStateException(
                    "make sure your application.properties has solr zookeeperhost or httphost properties");
        }
    }

    private HttpClient httpClient() {
        // Leo: I am creating HttpClient exactly in the same way it is created inside
        // CloudSolrClient.Builder,
        // but here I am just adding Credentials
        ModifiableSolrParams param = null;
        if (Utils.notNullNotEmpty(config.getUsername())
                && Utils.notNullNotEmpty(config.getPassword())) {
            param = new ModifiableSolrParams();
            param.add(HttpClientUtil.PROP_BASIC_AUTH_USER, config.getUsername());
            param.add(HttpClientUtil.PROP_BASIC_AUTH_PASS, config.getPassword());
        }
        return HttpClientUtil.createClient(param);
    }
}

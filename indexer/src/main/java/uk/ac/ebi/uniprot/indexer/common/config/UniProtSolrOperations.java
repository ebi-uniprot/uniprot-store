package uk.ac.ebi.uniprot.indexer.common.config;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrOperations;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.Query;
import org.springframework.data.solr.core.query.SolrDataQuery;

import java.util.Collection;
import java.util.Optional;

import static java.util.Arrays.asList;

/**
 * Created 10/07/19
 *
 * @author Edd
 */
public class UniProtSolrOperations {
    private final ThreadLocal<SolrOperations> threadLocalSolrOperations;
    private final RepositoryConfigProperties config;

    UniProtSolrOperations(RepositoryConfigProperties config) {
        this.config = config;
        this.threadLocalSolrOperations = ThreadLocal.withInitial(this::createSolrOperations);
    }

    public UniProtSolrOperations(SolrOperations solrOperations) {
        this.config = null;
        if (solrOperations instanceof SolrTemplate) {
            ((SolrTemplate) solrOperations).afterPropertiesSet();
        }
        this.threadLocalSolrOperations = ThreadLocal.withInitial(() -> solrOperations);
    }

    public <T, S extends Page<T>> S query(String var1, Query var2, Class<T> var3) {
        return threadLocalSolrOperations.get().query(var1, var2, var3);
    }

    public UpdateResponse saveBeans(String collection, Collection<?> beans) {
        return threadLocalSolrOperations.get().saveBeans(collection, beans);
    }

    public UpdateResponse saveBean(String collection, Object bean) {
        return threadLocalSolrOperations.get().saveBean(collection, bean);
    }

    public <T> Optional<T> queryForObject(String var1, Query var2, Class<T> var3) {
        return threadLocalSolrOperations.get().queryForObject(var1, var2, var3);
    }

    public void commit(String var1) {
        threadLocalSolrOperations.get().commit(var1);
    }

    public void softCommit(String var1) {
        threadLocalSolrOperations.get().softCommit(var1);
    }

    public UpdateResponse delete(String collection, SolrDataQuery query) {
        return threadLocalSolrOperations.get().delete(collection, query);
    }

    private SolrOperations createSolrOperations() {
        SolrTemplate solrTemplate = new SolrTemplate(uniProtSolrClient());
        solrTemplate.afterPropertiesSet();
        return solrTemplate;
    }

    private SolrClient uniProtSolrClient() {
        String zookeeperhost = config.getZkHost();
        if (zookeeperhost != null && !zookeeperhost.isEmpty()) {
            String[] zookeeperHosts = zookeeperhost.split(",");

            CloudSolrClient client = new CloudSolrClient.Builder(asList(zookeeperHosts), Optional.empty())
                    .withConnectionTimeout(config.getConnectionTimeout())
                    .withSocketTimeout(config.getSocketTimeout())
                    .build();

            client.setIdField("accession_id"); // TODO: 10/07/19 refactor schemas to include unique field 'id'
            return client;
        } else if (!config.getHttphost().isEmpty()) {
            return new HttpSolrClient.Builder().withHttpClient(httpClient()).withBaseSolrUrl(config.getHttphost())
                    .build();
        } else {
            throw new BeanCreationException("make sure your application.properties has eight solr zookeeperhost or httphost properties");
        }
    }

    private HttpClient httpClient() {
        // I am creating HttpClient exactly in the same way it is created inside CloudSolrClient.Builder,
        // but here I am just adding Credentials
        ModifiableSolrParams param = null;
        if (config.getUsername() != null && !config.getUsername().isEmpty() && config
                .getPassword() != null && !config
                .getPassword().isEmpty()) {
            param = new ModifiableSolrParams();
            param.add(HttpClientUtil.PROP_BASIC_AUTH_USER, config.getUsername());
            param.add(HttpClientUtil.PROP_BASIC_AUTH_PASS, config.getPassword());
        }
        return HttpClientUtil.createClient(param);
    }
}

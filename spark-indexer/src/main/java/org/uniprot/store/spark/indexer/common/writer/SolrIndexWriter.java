package org.uniprot.store.spark.indexer.common.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;

import java.util.Iterator;
import java.util.Optional;

import static java.util.Collections.singletonList;

/**
 * @author lgonzales
 * @since 03/09/2020
 */
@Slf4j
public class SolrIndexWriter implements VoidFunction<Iterator<SolrInputDocument>> {

    private static final long serialVersionUID = 1997175675889081522L;
    protected final String zkHost;
    protected final String collectionName;

    public SolrIndexWriter(String zkHost, String collectionName) {
        this.zkHost = zkHost;
        this.collectionName = collectionName;
    }

    @Override
    public void call(Iterator<SolrInputDocument> docs) throws Exception {
        try (SolrClient client = getSolrClient()) {
            client.add(collectionName, docs);
        } catch (Exception e) {
            String errorMessage =
                    "Exception indexing data to solr, for collection " + collectionName;
            log.error(errorMessage, e);
            throw new SolrIndexException(errorMessage, e);
        }
    }

    protected SolrClient getSolrClient(){
        return new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty())
                        .build();
    }
}

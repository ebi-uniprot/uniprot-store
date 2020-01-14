package org.uniprot.store.spark.indexer.util;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;

import com.lucidworks.spark.util.SolrSupport;

/**
 * @author lgonzales
 * @since 2019-11-03
 */
@Slf4j
public class SolrUtils {

    public static void saveSolrInputDocumentRDD(
            JavaPairRDD<String, ? extends Document> docRDD, String savePath) {
        docRDD.values()
                .map(
                        doc -> {
                            DocumentObjectBinder binder = new DocumentObjectBinder();
                            return binder.toSolrInputDocument(doc);
                        })
                .rdd()
                .saveAsObjectFile(savePath);
    }

    public static void indexDocuments(
            JavaRDD<SolrInputDocument> solrInputDocumentRDD,
            SolrCollection collection,
            ResourceBundle config) {
        String collectionName = collection.toString();
        String zkHost = config.getString("solr.zkhost");
        log.info("Starting solr index for collection: " + collectionName + " in zkHost " + zkHost);
        try {

            int solrBatchSize = Integer.valueOf(config.getString("solr.batch.size"));
            SolrSupport.indexDocs(
                    zkHost, collectionName, solrBatchSize, solrInputDocumentRDD.rdd());
            log.info("Completed solr index for collection " + collectionName);
        } catch (Exception e) {
            log.error("Exception indexing data to solr, for collection " + collectionName, e);
            throw new RuntimeException("Exception: ", e);
        }
        log.info("Completed solr index for collection " + collectionName);

        CloudSolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        commit(collectionName, solrClient);
        optimize(collectionName, solrClient);
    }

    private static void optimize(String collection, CloudSolrClient solrClient) {
        log.info("Optimizing the data for collection " + collection);
        try {
            solrClient.optimize(collection, true, true);
        } catch (Exception e) {
            try {
                solrClient.optimize(collection, true, true);
            } catch (Exception ce) {
                log.error("Unable to optimize the data for collection " + collection, ce);
            }
        }
    }

    private static void commit(String collection, CloudSolrClient solrClient) {
        log.info("Commiting the data for collection " + collection);
        try {
            solrClient.commit(collection, true, true);
        } catch (Exception e) {
            log.error("Error commiting the data for collection, tentative 1 " + collection, e);
            try {
                solrClient.commit(collection, true, true);
            } catch (Exception ce) {
                throw new RuntimeException("Unable to commit in solr after two tentatives: ", ce);
            }
        }
    }
}

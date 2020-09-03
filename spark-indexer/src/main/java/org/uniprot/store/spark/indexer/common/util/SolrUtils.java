package org.uniprot.store.spark.indexer.common.util;

import static java.util.Collections.*;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.store.search.document.Document;

/**
 * @author lgonzales
 * @since 2019-11-03
 */
@Slf4j
public class SolrUtils {

    private SolrUtils() {}

    public static void saveSolrInputDocumentRDD(
            JavaPairRDD<String, ? extends Document> docRDD, String savePath) {
        saveSolrInputDocumentRDD(docRDD.values(), savePath);
    }

    public static void saveSolrInputDocumentRDD(
            JavaRDD<? extends Document> docRDD, String savePath) {
        docRDD.map(SolrUtils::convertToSolrInputDocument).rdd().saveAsObjectFile(savePath);
    }

    public static SolrInputDocument convertToSolrInputDocument(Document doc) {
        DocumentObjectBinder binder = new DocumentObjectBinder();
        return binder.toSolrInputDocument(doc);
    }

    public static void commit(String collection, String zkHost) {
        log.info("Committing the data for collection " + collection);
        try (CloudSolrClient client =
                new CloudSolrClient.Builder(singletonList(zkHost), Optional.empty()).build()) {
            client.commit(collection, true, true);
        } catch (Exception e) {
            log.error("Error committing the data for collection" + collection, e);
        }
        log.info("Completed commit the data for collection " + collection);
    }
}

package org.uniprot.store.spark.indexer.common.util;

import static java.util.Collections.singletonList;

import java.util.Optional;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.store.search.document.Document;

import lombok.extern.slf4j.Slf4j;

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
        if (doc == null) {
            throw new IllegalArgumentException("Cannot convert null document to SolrInputDocument");
        }
        DocumentObjectBinder binder = new DocumentObjectBinder();
        try {
            return binder.toSolrInputDocument(doc);
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "Unable to convert document to SolrInputDocument. documentType="
                            + doc.getClass().getName()
                            + ", documentId="
                            + getDocumentId(doc),
                    e);
        }
    }

    private static String getDocumentId(Document doc) {
        try {
            return doc.getDocumentId();
        } catch (RuntimeException e) {
            return "<unavailable>";
        }
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

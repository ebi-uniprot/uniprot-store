package indexer.util;

import com.lucidworks.spark.util.SolrSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import org.uniprot.store.search.document.Document;

import java.io.IOException;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-11-03
 */
@Slf4j
public class SolrUtils {

    public static void indexDocuments(JavaPairRDD<String, ? extends Document> docRDD, String collection, ResourceBundle config) {
        try {
            RDD<SolrInputDocument> solrInputDocumentRDD = docRDD.values().map(doc -> {
                DocumentObjectBinder binder = new DocumentObjectBinder();
                return binder.toSolrInputDocument(doc);
            }).rdd();

            String zkHost = config.getString("solr.zkhost");
            int solrBatchSize = Integer.valueOf(config.getString("solr.batch.size"));
            SolrSupport.indexDocs(zkHost, collection, solrBatchSize, solrInputDocumentRDD);

            CloudSolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
            solrClient.commit(collection, true, true);
            solrClient.optimize(collection, true, true);
            log.info("Completed solr index for collection " + collection);
        } catch (SolrServerException e) {
            log.error("SolrServerException indexing data to solr, for collection " + collection, e);
        } catch (IOException e) {
            log.error("IOException indexing data to solr, for collection " + collection, e);
        } catch (Exception e) {
            log.error("Exception indexing data to solr, for collection " + collection, e);
        }
    }
}

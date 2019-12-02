package indexer;

import java.util.ResourceBundle;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import indexer.util.SolrUtils;
import indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-11-07
 */
public class IndexRDDInSolr {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        String hdfsFilePath =
                applicationConfig.getString(
                        "uniprot.solr.documents.path"); // applicationConfig.getString(args[0]);
        JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                (JavaRDD<SolrInputDocument>)
                        sparkContext
                                .objectFile(hdfsFilePath)
                                .map(
                                        obj -> {
                                            return (SolrInputDocument) obj;
                                        });

        String solrCollection = "uniprot"; // args[1];
        SolrUtils.indexDocuments(solrInputDocumentRDD, solrCollection, applicationConfig);

        sparkContext.close();
    }
}

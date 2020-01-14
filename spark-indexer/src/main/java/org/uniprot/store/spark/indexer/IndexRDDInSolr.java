package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.util.SolrUtils;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * This class is responsible to load data from saved SolrDocuments and Index in Solr
 *
 * @author lgonzales
 * @since 2019-11-07
 */
public class IndexRDDInSolr {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        if (args == null || args.length < 2) {
            throw new RuntimeException(
                    "Invalid arguments. "
                            + "Expected args[0]=hdfsFilePath parameter name "
                            + " (for example: uniprot.solr.documents.path), "
                            + "args[1]= collection name (for example: uniprot)");
        }

        String hdfsFilePath = getHDFSFilePath(args, applicationConfig);
        JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                (JavaRDD<SolrInputDocument>)
                        sparkContext
                                .objectFile(hdfsFilePath)
                                .map(
                                        obj -> {
                                            return (SolrInputDocument) obj;
                                        });

        SolrCollection solrCollection = getSolrCollection(args);
        SolrUtils.indexDocuments(solrInputDocumentRDD, solrCollection, applicationConfig);

        sparkContext.close();
    }

    private static SolrCollection getSolrCollection(String[] args) {
        try {
            return SolrCollection.valueOf(args[1]);
        } catch (Exception e) {
            throw new RuntimeException("Invalid solr collection name: " + args[1]);
        }
    }

    private static String getHDFSFilePath(String[] args, ResourceBundle applicationConfig) {
        try {
            return applicationConfig.getString(args[0]);
            // applicationConfig.getString("uniprot.solr.documents.path");
        } catch (Exception e) {
            throw new RuntimeException(
                    "Invalid hdfsFilePath parameter name: "
                            + args[0]
                            + "Expected uniprot.solr.documents.path for example",
                    e);
        }
    }
}

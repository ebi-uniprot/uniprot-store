package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.suggest.SuggestIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBIndexer;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class WriteIndexDocumentsToHDFSMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected args[0]= collection name (for example: uniprot)");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            SolrCollection solrCollection = SparkUtils.getSolrCollection(args[0]);
            switch (solrCollection) {
                case uniprot:
                    UniProtKBIndexer.writeIndexDocumentsToHDFS(sparkContext, applicationConfig);
                    break;
                case suggest:
                    SuggestIndexer.writeIndexDocumentsToHDFS(sparkContext, applicationConfig);
                    break;
                default:
                    throw new RuntimeException("Collection not yet supported by spark indexer");
            }
        } catch (Exception e) {
            throw new Exception("Unexpected error during index", e);
        }
    }
}

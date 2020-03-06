package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.uniparc.UniParcDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefDataStoreIndexer;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
public class IndexDataStoreMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= collection name (for example: uniprot,uniparc,uniref) "
                            + "args[1]= release name");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            String dataStore = args[0].toLowerCase();
            String releaseName = args[1];
            switch (dataStore) {
                case "uniparc":
                    UniParcDataStoreIndexer.indexDataStore(
                            sparkContext, applicationConfig, releaseName);
                    break;
                case "uniref":
                    UniRefDataStoreIndexer.indexDataStore(
                            sparkContext, applicationConfig, releaseName);
                    break;
                case "uniprot":
                    UniProtKBDataStoreIndexer.indexDataStore(
                            sparkContext, applicationConfig, releaseName);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Data Store '" + dataStore + "' not yet supported by spark indexer");
            }
        } catch (Exception e) {
            throw new Exception("Unexpected error during index", e);
        }
    }
}

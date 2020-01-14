package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBIndexer;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class SparkRDDDriverProgram {

    public static void main(String[] args) throws Exception {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        UniProtKBIndexer.prepareSolrIndex(sparkContext, applicationConfig);

        sparkContext.close();
    }
}

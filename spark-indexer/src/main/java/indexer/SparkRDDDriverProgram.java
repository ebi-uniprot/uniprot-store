package indexer;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;

import indexer.uniprot.UniprotKbIndexer;
import indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class SparkRDDDriverProgram {

    public static void main(String[] args) throws Exception {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        UniprotKbIndexer.prepareSolrIndex(sparkContext, applicationConfig);

        sparkContext.close();
    }
}

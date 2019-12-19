package indexer;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-12-12
 */
@Slf4j
public class ValidateSavedDocument {

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

        solrInputDocumentRDD
                .take(200)
                .forEach(
                        solrInputFields -> {
                            log.info(
                                    "----------------------------------------------------------------------");
                            solrInputFields.forEach(
                                    (key, value) ->
                                            log.info(
                                                    "FIELD:" + key + " VALUE:" + value.toString()));
                        });
        sparkContext.close();
    }
}

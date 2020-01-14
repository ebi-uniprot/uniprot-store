package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * This class is used to print first 200 solr document saved in HDFS. it is helpful during the
 * development phase, because you can visualize what is being saved in HDFS.
 *
 * @author lgonzales
 * @since 2019-12-12
 */
@Slf4j
public class ValidateSavedDocument {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        String hdfsFilePath = applicationConfig.getString(args[0]);
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

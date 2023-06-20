package org.uniprot.store.spark.indexer.main.verifiers;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getSolrCollection;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * This class is used to print first 200 solr document saved in HDFS. it is helpful during the
 * development phase, because you can visualize what is being saved in HDFS.
 *
 * @author lgonzales
 * @since 2019-12-12
 */
@Slf4j
public class ValidateHDFSDocumentsMain {

    public static void main(String[] args) {
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        SolrCollection collection = getSolrCollection(args[1]).get(0);
        String hdfsFilePath =
                getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);
        log.info("Output Documents Path: {}", hdfsFilePath);
        JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                sparkContext.objectFile(hdfsFilePath).map(obj -> (SolrInputDocument) obj);

        log.info("Documents Count: {}", solrInputDocumentRDD.count());
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

package org.uniprot.store.spark.indexer.validator;

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
 * This class is used to print first 200 solr document saved in HPS. it is helpful during the
 * development phase, because you can visualize what is being saved in HPS.
 *
 * @author lgonzales
 * @since 2019-12-12
 */
@Slf4j
public class ValidateHPSDocumentsMain {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid argument. Expected "
                            + "args[0]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)");
        }
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig, args[0]);

        SolrCollection collection = getSolrCollection(args[1]).get(0);
        String hpsFilePath =
                getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);
        log.info("Output Documents Path: {}", hpsFilePath);
        JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                sparkContext.objectFile(hpsFilePath).map(obj -> (SolrInputDocument) obj);

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

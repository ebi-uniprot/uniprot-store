package org.uniprot.store.spark.indexer.main;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.InactiveUniProtKBRDDTupleReader;

/**
 * @author lgonzales
 * @since 29/05/2020
 */
@Slf4j
public class IndexInactive {

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected " + "args[0]= release name");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            JavaRDD<SolrInputDocument> inactiveRDD =
                    InactiveUniProtKBRDDTupleReader.load(jobParameter)
                            .values()
                            .map(SolrUtils::convertToSolrInputDocument);

            SolrUtils.indexDocuments(inactiveRDD, SolrCollection.uniprot, applicationConfig);
        } catch (Exception e) {
            throw new SolrIndexException("Unexpected error during Solr index", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }
}

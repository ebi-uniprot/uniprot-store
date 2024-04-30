package org.uniprot.store.spark.indexer.main;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriterFactory;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
@Slf4j
public class WriteIndexDocumentsToHPSMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 3) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01"
                            + "args[1]= collection name (for example: uniprot, uniparc, uniref or suggest)"
                            + "args[2]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)");
        }

        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, args[2])) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            List<SolrCollection> solrCollections = SparkUtils.getSolrCollection(args[1]);
            DocumentsToHPSWriterFactory factory = new DocumentsToHPSWriterFactory();
            for (SolrCollection collection : solrCollections) {
                DocumentsToHPSWriter writer =
                        factory.createDocumentsToHPSWriter(collection, jobParameter);
                writer.writeIndexDocumentsToHPS();
            }
        } catch (Exception e) {
            throw new IndexHPSDocumentsException("Unexpected error during index", e);
        } finally {
            log.info("Finished all Jobs!!!");
        }
    }
}

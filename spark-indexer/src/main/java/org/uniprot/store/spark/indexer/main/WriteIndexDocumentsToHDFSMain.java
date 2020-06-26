package org.uniprot.store.spark.indexer.main;

import java.util.List;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriterFactory;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
@Slf4j
public class WriteIndexDocumentsToHDFSMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name (for example: 2020_01"
                            + "args[1]= collection name (for example: uniprot, uniparc, uniref or suggest)");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();
            List<SolrCollection> solrCollections = SparkUtils.getSolrCollection(args[1]);
            for (SolrCollection collection : solrCollections) {
                DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
                DocumentsToHDFSWriter writer =
                        factory.createDocumentsToHDFSWriter(collection, jobParameter);
                writer.writeIndexDocumentsToHDFS();
            }
        } catch (Exception e) {
            throw new IndexHDFSDocumentsException("Unexpected error during index", e);
        } finally {
            log.info("Finished all Jobs!!!");
        }
    }
}

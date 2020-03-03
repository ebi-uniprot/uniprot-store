package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriterFactory;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class WriteIndexDocumentsToHDFSMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= collection name (for example: uniprot, uniparc, uniref or suggest) "
                            + "args[1]= release name ?(for example: 2020_01");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            SolrCollection solrCollection = SparkUtils.getSolrCollection(args[0]);
            String releaseName = args[1];

            DocumentsToHDFSWriterFactory factory = new DocumentsToHDFSWriterFactory();
            DocumentsToHDFSWriter writer = factory.createDocumentsToHDFSWriter(solrCollection);
            writer.writeIndexDocumentsToHDFS(sparkContext, releaseName);

        } catch (Exception e) {
            throw new Exception("Unexpected error during index", e);
        }
    }
}

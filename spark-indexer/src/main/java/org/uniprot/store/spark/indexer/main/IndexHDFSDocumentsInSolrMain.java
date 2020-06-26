package org.uniprot.store.spark.indexer.main;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import java.util.List;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;

/**
 * This class is responsible to load data from saved SolrDocuments and Index in Solr
 *
 * @author lgonzales
 * @since 2019-11-07
 */
@Slf4j
public class IndexHDFSDocumentsInSolrMain {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. "
                            + "Expected args[0]=release name (for example: (for example: 2020_02), "
                            + "args[1]= collection names comma separated (for example: uniprot,suggest)");
        }

        ResourceBundle applicationConfig = loadApplicationProperty();
        try (JavaSparkContext sparkContext = loadSparkContext(applicationConfig)) {

            List<SolrCollection> solrCollections = getSolrCollection(args[1]);
            for (SolrCollection collection : solrCollections) {
                String hdfsFilePath =
                        getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);
                JavaRDD<SolrInputDocument> solrInputDocumentRDD =
                        sparkContext.objectFile(hdfsFilePath).map(obj -> (SolrInputDocument) obj);

                SolrUtils.indexDocuments(solrInputDocumentRDD, collection, applicationConfig);
            }
        } catch (Exception e) {
            throw new SolrIndexException("Unexpected error while index in solr", e);
        } finally {
            log.info("Finished the job!!");
        }
    }
}

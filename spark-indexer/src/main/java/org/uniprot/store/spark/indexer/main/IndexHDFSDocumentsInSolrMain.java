package org.uniprot.store.spark.indexer.main;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import java.util.List;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.SolrIndexParameter;
import org.uniprot.store.spark.indexer.common.writer.SolrIndexWriter;

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
        String zkHost = applicationConfig.getString("solr.zkhost");
        try (JavaSparkContext sparkContext = loadSparkContext(applicationConfig)) {

            List<SolrCollection> solrCollections = getSolrCollection(args[1]);
            for (SolrCollection collection : solrCollections) {
                String hdfsFilePath =
                        getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);

                log.info(
                        "Started solr index for collection: "
                                + collection.name()
                                + " in zkHost "
                                + zkHost);
                SolrIndexParameter indexParameter =
                        getSolrIndexParameter(collection, applicationConfig);
                sparkContext
                        .objectFile(hdfsFilePath)
                        .map(obj -> (SolrInputDocument) obj)
                        .foreachPartition(new SolrIndexWriter(indexParameter));
                log.info(
                        "Completed solr index for collection: "
                                + collection.name()
                                + " in zkHost "
                                + zkHost);

                SolrUtils.commit(collection.name(), zkHost);
            }
        } catch (Exception e) {
            throw new SolrIndexException("Unexpected error while index in solr", e);
        } finally {
            log.info("Finished the job!!");
        }
    }

    static SolrIndexParameter getSolrIndexParameter(
            SolrCollection collection, ResourceBundle config) {
        String delay = config.getString("solr.retry.delay");
        String maxRetry = config.getString("solr.max.retry");
        return SolrIndexParameter.builder()
                .collectionName(collection.name())
                .zkHost(config.getString("solr.zkhost"))
                .delay(Long.parseLong(delay))
                .maxRetry(Integer.parseInt(maxRetry))
                .build();
    }
}

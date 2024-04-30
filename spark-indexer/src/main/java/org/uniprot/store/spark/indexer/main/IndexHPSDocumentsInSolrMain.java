package org.uniprot.store.spark.indexer.main;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.SolrIndexParameter;
import org.uniprot.store.spark.indexer.common.writer.SolrIndexWriter;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible to load data from saved SolrDocuments and Index in Solr
 *
 * @author lgonzales
 * @since 2019-11-07
 */
@Slf4j
public class IndexHPSDocumentsInSolrMain {

    public static void main(String[] args) {
        if (args == null || args.length != 3) {
            throw new IllegalArgumentException(
                    "Invalid arguments. "
                            + "Expected args[0]=release name (for example: (for example: 2020_02), "
                            + "args[1]= collection names comma separated (for example: uniprot,suggest)"
                            + "args[2]=spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)");
        }

        Config applicationConfig = loadApplicationProperty();
        String zkHost = applicationConfig.getString("solr.zkhost");
        try (JavaSparkContext sparkContext = loadSparkContext(applicationConfig, args[2])) {

            List<SolrCollection> solrCollections = getSolrCollection(args[1]);
            for (SolrCollection collection : solrCollections) {
                String hpsFilePath =
                        getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);

                log.info(
                        "Started solr index for collection: "
                                + collection.name()
                                + " in zkHost "
                                + zkHost);
                SolrIndexParameter indexParameter =
                        getSolrIndexParameter(collection, applicationConfig);
                sparkContext
                        .objectFile(hpsFilePath)
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

    static SolrIndexParameter getSolrIndexParameter(SolrCollection collection, Config config) {
        String delay = config.getString("solr.retry.delay");
        String maxRetry = config.getString("solr.max.retry");
        String batchSize = config.getString("solr.index.batch.size");
        return SolrIndexParameter.builder()
                .collectionName(collection.name())
                .zkHost(config.getString("solr.zkhost"))
                .delay(Long.parseLong(delay))
                .maxRetry(Integer.parseInt(maxRetry))
                .batchSize(Integer.parseInt(batchSize))
                .build();
    }
}

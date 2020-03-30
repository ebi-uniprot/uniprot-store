package org.uniprot.store.spark.indexer;

import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.uniparc.UniParcDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefDataStoreIndexer;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class IndexDataStoreMain {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]= collection names (for example: uniprot,uniparc,uniref)");
        }

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        String releaseName = args[0];
        String[] dataStoreArg = args[1].toLowerCase().split(",");
        ExecutorService executorService = Executors.newFixedThreadPool(dataStoreArg.length);
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            for (String dataStore : dataStoreArg) {
                switch (dataStore) {
                    case "uniparc":
                        UniParcDataStoreIndexer uniParcIndexer =
                                new UniParcDataStoreIndexer(
                                        sparkContext, applicationConfig, releaseName);
                        executorService.execute(uniParcIndexer);
                        break;
                    case "uniref":
                        UniRefDataStoreIndexer uniRefIndexer =
                                new UniRefDataStoreIndexer(
                                        sparkContext, applicationConfig, releaseName);
                        executorService.execute(uniRefIndexer);
                        break;
                    case "uniprot":
                        UniProtKBDataStoreIndexer uniProtKBIndexer =
                                new UniProtKBDataStoreIndexer(
                                        sparkContext, applicationConfig, releaseName);
                        executorService.execute(uniProtKBIndexer);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Data Store '"
                                        + dataStore
                                        + "' not yet supported by spark indexer");
                }
            }
            executorService.shutdown();
            log.info("Submitted all jobs");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            log.info("All jobs finished!!!");
        } catch (Exception e) {
            throw new Exception("Unexpected error during index", e);
        } finally {
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }
}

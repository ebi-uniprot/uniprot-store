package org.uniprot.store.spark.indexer.main.experimental;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexerFactory;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 28/04/2020
 */
@Slf4j
public class IndexDataStoreMainMultiThread {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]= collection names (for example: uniprot,uniparc,uniref)");
        }

        Config applicationConfig = SparkUtils.loadApplicationProperty();
        List<DataStore> dataStores = SparkUtils.getDataStores(args[1]);
        ExecutorService executorService = Executors.newFixedThreadPool(dataStores.size());
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();

            DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
            for (DataStore dataStore : dataStores) {
                executorService.execute(
                        () -> {
                            DataStoreIndexer dataStoreIndexer =
                                    factory.createDataStoreIndexer(dataStore, jobParameter);
                            dataStoreIndexer.indexInDataStore();
                        });
            }

            executorService.shutdown();
            log.info("Submitted all jobs");
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            log.info("All jobs finished!!!");
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore index", e);
        } finally {
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }
}

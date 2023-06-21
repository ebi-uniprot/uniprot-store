package org.uniprot.store.spark.indexer.main;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexerFactory;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * This class is responsible to load data into our data store (voldemort)
 *
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class IndexDataStoreMain {

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]= collection names (for example: uniprot,uniparc,uniref)");
        }
        log.info("Starting to read config");
        Config applicationConfig = SparkUtils.loadApplicationProperty();
        log.info("After read config");
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            log.info("################# config ####################");
            applicationConfig.entrySet()
                    .forEach(e -> log.info(e.getKey() + "=" + applicationConfig.getString(e.getKey())));
            log.info("################# config done ####################");
            log.info("After creating context");
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .sparkContext(sparkContext)
                            .build();

            DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
            List<DataStore> dataStores = SparkUtils.getDataStores(args[1]);
            for (DataStore dataStore : dataStores) {
                log.info("Indexing data store: " + dataStore.getName());
                //                DataStoreIndexer dataStoreIndexer =
                //                        factory.createDataStoreIndexer(dataStore, jobParameter);
                //                dataStoreIndexer.indexInDataStore();
            }
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore index", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }
}

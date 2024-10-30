package org.uniprot.store.spark.indexer.main;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexerFactory;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import java.util.List;

import static org.uniprot.store.spark.indexer.common.TaxDb.forName;

/**
 * This class is responsible to load data into our data store (voldemort)
 *
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class IndexDataStoreMain {

    public static void main(String[] args) {
        if (args == null || args.length != 4) {
            throw new IllegalArgumentException(
                    "Invalid arguments. Expected "
                            + "args[0]= release name"
                            + "args[1]= collection names (for example: uniprot,uniparc,uniref)"
                            + "args[2]= spark master node url (e.g. spark://hl-codon-102-02.ebi.ac.uk:37550)"
                            + "args[3]= taxonomy db (e.g.read or fly)");
        }
        log.info("release name " + args[0]);
        log.info("collection name " + args[1]);
        log.info("spark master node url " + args[2]);
        log.info("taxonomy db " + args[3]);

        Config applicationConfig = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(applicationConfig, args[2])) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .applicationConfig(applicationConfig)
                            .releaseName(args[0])
                            .taxDb(forName(args[3]))
                            .sparkContext(sparkContext)
                            .build();

            DataStoreIndexerFactory factory = new DataStoreIndexerFactory();
            List<DataStore> dataStores = SparkUtils.getDataStores(args[1]);
            for (DataStore dataStore : dataStores) {
                log.info("Indexing data store: " + dataStore.getName());
                DataStoreIndexer dataStoreIndexer =
                        factory.createDataStoreIndexer(dataStore, jobParameter);
                dataStoreIndexer.indexInDataStore();
            }
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore index", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }
}

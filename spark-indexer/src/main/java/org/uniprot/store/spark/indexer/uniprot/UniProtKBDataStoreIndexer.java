package org.uniprot.store.spark.indexer.uniprot;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniProtKBDataStoreIndexer implements DataStoreIndexer {

    private final ResourceBundle config;
    private final String releaseName;
    private final JavaSparkContext sparkContext;

    public UniProtKBDataStoreIndexer(JobParameter parameter) {
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
        this.sparkContext = parameter.getSparkContext();
    }

    @Override
    public void indexInDataStore() {
        JavaPairRDD<String, UniProtKBEntry> uniprotRDD =
                UniProtKBRDDTupleReader.load(sparkContext, config, releaseName);

        String numberOfConnections = config.getString("store.uniprot.numberOfConnections");
        String storeName = config.getString("store.uniprot.storeName");
        String connectionURL = config.getString("store.uniprot.host");

        uniprotRDD
                .values()
                .foreachPartition(
                        uniProtEntryIterator -> {
                            VoldemortClient<UniProtKBEntry> client =
                                    new VoldemortRemoteUniProtKBEntryStore(
                                            Integer.parseInt(numberOfConnections),
                                            storeName,
                                            connectionURL);
                            DataStoreWriter<UniProtKBEntry> writer = new DataStoreWriter<>(client);
                            writer.indexInStore(uniProtEntryIterator);
                        });
        log.info("Completed UniProtKb Data Store index");
    }
}

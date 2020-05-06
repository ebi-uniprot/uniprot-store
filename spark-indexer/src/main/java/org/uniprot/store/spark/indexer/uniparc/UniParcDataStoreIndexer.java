package org.uniprot.store.spark.indexer.uniparc;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class UniParcDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniParcDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        ResourceBundle config = parameter.getApplicationConfig();
        JavaRDD<UniParcEntry> uniparcRDD = UniParcRDDTupleReader.load(parameter, false);

        String numberOfConnections = config.getString("store.uniparc.numberOfConnections");
        String storeName = config.getString("store.uniparc.storeName");
        String connectionURL = config.getString("store.uniparc.host");

        uniparcRDD.foreachPartition(
                uniProtEntryIterator -> {
                    try (VoldemortClient<UniParcEntry> client =
                            new VoldemortRemoteUniParcEntryStore(
                                    Integer.parseInt(numberOfConnections),
                                    storeName,
                                    connectionURL)) {
                        DataStoreWriter<UniParcEntry> writer = new DataStoreWriter<>(client);
                        writer.indexInStore(uniProtEntryIterator);
                    } finally {
                        log.info("Closed voldemort connection");
                    }
                });
        log.info("Completed UniParc Data Store index");
    }
}

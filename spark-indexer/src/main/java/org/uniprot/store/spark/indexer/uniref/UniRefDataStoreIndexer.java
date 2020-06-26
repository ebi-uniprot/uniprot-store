package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 2020-03-06
 */
@Slf4j
public class UniRefDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter jobParameter;

    public UniRefDataStoreIndexer(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public void indexInDataStore() {
        try {
            JavaFutureAction<Void> uniref100 = indexUniRef(UniRefType.UniRef100, jobParameter);
            JavaFutureAction<Void> uniref90 = indexUniRef(UniRefType.UniRef90, jobParameter);
            JavaFutureAction<Void> uniref50 = indexUniRef(UniRefType.UniRef50, jobParameter);
            uniref50.get();
            uniref90.get();
            uniref100.get();
        } catch (Exception e) {
            throw new IndexDataStoreException("Execution error during DataStore index", e);
        } finally {
            log.info("Completed UniRef Data Store index");
        }
    }

    private JavaFutureAction<Void> indexUniRef(UniRefType type, JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();

        final String numberOfConnections = config.getString("store.uniref.numberOfConnections");
        final String storeName = config.getString("store.uniref.storeName");
        final String connectionURL = config.getString("store.uniref.host");

        UniRefRDDTupleReader reader = new UniRefRDDTupleReader(type, jobParameter, false);
        JavaRDD<UniRefEntry> uniRefRDD = reader.load();
        return uniRefRDD.foreachPartitionAsync(
                entryIterator -> {
                    VoldemortClient<UniRefEntry> client =
                            new VoldemortRemoteUniRefEntryStore(
                                    Integer.parseInt(numberOfConnections),
                                    storeName,
                                    connectionURL);
                    DataStoreWriter<UniRefEntry> writer = new DataStoreWriter<>(client);
                    writer.indexInStore(entryIterator);
                });
    }
}

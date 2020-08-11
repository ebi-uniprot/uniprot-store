package org.uniprot.store.spark.indexer.uniref.writer;

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
public class UniRefDataStoreWriter implements VoidFunction<Iterator<UniRefEntry>> {

    private final int numberOfConnections;
    private final String storeName;
    private final String connectionURL;

    public UniRefDataStoreWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        this.numberOfConnections = Integer.parseInt(numberOfConnections);
        this.storeName = storeName;
        this.connectionURL = connectionURL;
    }

    @Override
    public void call(Iterator<UniRefEntry> entryIterator) throws Exception {
        VoldemortClient<UniRefEntry> client = getDataStoreClient();
        DataStoreWriter<UniRefEntry> writer = new DataStoreWriter<>(client);
        writer.indexInStore(entryIterator);
    }

    VoldemortClient<UniRefEntry> getDataStoreClient() {
        return new VoldemortRemoteUniRefEntryStore(numberOfConnections, storeName, connectionURL);
    }
}

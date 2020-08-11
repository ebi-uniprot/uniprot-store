package org.uniprot.store.spark.indexer.uniref.writer;

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortRemoteUniRefEntryLightStore;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
public class UniRefLightDataStoreWriter implements VoidFunction<Iterator<UniRefEntryLight>> {

    private final int numberOfConnections;
    private final String storeName;
    private final String connectionURL;

    public UniRefLightDataStoreWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        this.numberOfConnections = Integer.parseInt(numberOfConnections);
        this.connectionURL = connectionURL;
        this.storeName = storeName;
    }

    @Override
    public void call(Iterator<UniRefEntryLight> entryIterator) throws Exception {
        VoldemortClient<UniRefEntryLight> client = getDataStoreClient();
        DataStoreWriter<UniRefEntryLight> writer = new DataStoreWriter<>(client);
        writer.indexInStore(entryIterator);
    }

    VoldemortClient<UniRefEntryLight> getDataStoreClient() {
        return new VoldemortRemoteUniRefEntryLightStore(
                numberOfConnections, storeName, connectionURL);
    }
}

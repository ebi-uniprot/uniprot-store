package org.uniprot.store.spark.indexer.common.writer;

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.datastore.voldemort.VoldemortClient;

/**
 * This class is responsible to write an RDD partition (Entry Iterator) into our DataStore
 * @author lgonzales
 * @since 30/07/2020
 */
public abstract class AbstractDataStoreWriter<T> implements VoidFunction<Iterator<T>> {

    private static final long serialVersionUID = -6006659302869170050L;
    protected final int numberOfConnections;
    protected final String storeName;
    protected final String connectionURL;

    public AbstractDataStoreWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        this.numberOfConnections = Integer.parseInt(numberOfConnections);
        this.storeName = storeName;
        this.connectionURL = connectionURL;
    }

    @Override
    public void call(Iterator<T> entryIterator) throws Exception {
        VoldemortClient<T> client = getDataStoreClient();
        DataStoreWriter<T> writer = new DataStoreWriter<>(client);
        writer.indexInStore(entryIterator);
    }

    protected abstract VoldemortClient<T> getDataStoreClient();
}

package org.uniprot.store.spark.indexer.uniref.writer;

import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;
import org.uniprot.store.spark.indexer.common.writer.DataStoreWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefMembersDataStoreIndexer;

/**
 * This class wraps logic to write a partition of data to voldemort. See {@link
 * UniRefMembersDataStoreIndexer#indexInDataStore()} and call to foreachPartition()
 *
 * @author sahmad
 * @since 21/07/2020
 */
public class UniRefMemberDataStoreWriter implements VoidFunction<Iterator<RepresentativeMember>> {

    private final int numberOfConnections;
    private final String storeName;
    private final String connectionURL;

    public UniRefMemberDataStoreWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        this.numberOfConnections = Integer.parseInt(numberOfConnections);
        this.connectionURL = connectionURL;
        this.storeName = storeName;
    }

    @Override
    public void call(Iterator<RepresentativeMember> entryIterator) throws Exception {
        VoldemortClient<RepresentativeMember> client = getDataStoreClient();
        DataStoreWriter<RepresentativeMember> writer = new DataStoreWriter<>(client);
        writer.indexInStore(entryIterator);
    }

    VoldemortClient<RepresentativeMember> getDataStoreClient() {
        return new VoldemortRemoteUniRefMemberStore(numberOfConnections, storeName, connectionURL);
    }
}

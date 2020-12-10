package org.uniprot.store.spark.indexer.uniref.writer;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.member.uniref.VoldemortRemoteUniRefMemberStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;
import org.uniprot.store.spark.indexer.uniref.UniRefMembersDataStoreIndexer;

/**
 * This class wraps logic to write a partition of data to voldemort. See {@link
 * UniRefMembersDataStoreIndexer#indexInDataStore()} and call to foreachPartition()
 *
 * @author sahmad
 * @since 21/07/2020
 */
public class UniRefMemberDataStoreWriter extends AbstractDataStoreWriter<RepresentativeMember> {

    private static final long serialVersionUID = -4513569352067190426L;

    public UniRefMemberDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    public VoldemortClient<RepresentativeMember> getDataStoreClient() {
        return new VoldemortRemoteUniRefMemberStore(
                parameter.getNumberOfConnections(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

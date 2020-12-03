package org.uniprot.store.spark.indexer.uniref.writer;

import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortRemoteUniRefEntryLightStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
public class UniRefLightDataStoreWriter extends AbstractDataStoreWriter<UniRefEntryLight> {

    private static final long serialVersionUID = -5255798510138980540L;

    public UniRefLightDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    public VoldemortClient<UniRefEntryLight> getDataStoreClient() {
        return new VoldemortRemoteUniRefEntryLightStore(
                parameter.getNumberOfConnections(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

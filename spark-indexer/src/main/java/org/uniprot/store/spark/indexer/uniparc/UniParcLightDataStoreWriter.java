package org.uniprot.store.spark.indexer.uniparc;

import java.io.Serial;

import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.VoldemortRemoteUniParcEntryLightStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

public class UniParcLightDataStoreWriter extends AbstractDataStoreWriter<UniParcEntryLight> {

    @Serial private static final long serialVersionUID = -5970975427885850402L;

    public UniParcLightDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    public VoldemortClient<UniParcEntryLight> getDataStoreClient() {
        return new VoldemortRemoteUniParcEntryLightStore(
                parameter.getNumberOfConnections(),
                parameter.isBrotliEnabled(),
                parameter.getBrotliLevel(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

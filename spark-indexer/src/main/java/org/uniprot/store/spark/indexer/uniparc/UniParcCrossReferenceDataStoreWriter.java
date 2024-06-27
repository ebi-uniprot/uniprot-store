package org.uniprot.store.spark.indexer.uniparc;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

public class UniParcCrossReferenceDataStoreWriter
        extends AbstractDataStoreWriter<UniParcCrossReference> {

    public UniParcCrossReferenceDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    public VoldemortClient<UniParcCrossReference> getDataStoreClient() {
        return new VoldemortRemoteUniParcCrossReferenceStore(
                parameter.getNumberOfConnections(),
                parameter.isBrotliEnabled(),
                parameter.getBrotliLevel(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

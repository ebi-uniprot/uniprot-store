package org.uniprot.store.spark.indexer.uniparc;

import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniparc.VoldemortRemoteUniParcEntryStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

/**
 * @author lgonzales
 * @since 02/12/2020
 */
public class UniParcDataStoreWriter extends AbstractDataStoreWriter<UniParcEntry> {

    private static final long serialVersionUID = 5594214191877952028L;

    public UniParcDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    public VoldemortClient<UniParcEntry> getDataStoreClient() {
        return new VoldemortRemoteUniParcEntryStore(
                parameter.getNumberOfConnections(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

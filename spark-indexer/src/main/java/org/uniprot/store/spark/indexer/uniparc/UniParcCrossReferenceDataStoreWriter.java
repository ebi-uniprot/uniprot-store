package org.uniprot.store.spark.indexer.uniparc;

import java.io.Serial;

import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UniParcCrossReferenceDataStoreWriter
        extends AbstractDataStoreWriter<UniParcCrossReferencePair> {
    @Serial private static final long serialVersionUID = -5970975427885850402L;

    public UniParcCrossReferenceDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    protected VoldemortClient<UniParcCrossReferencePair> getDataStoreClient() {
        return new VoldemortRemoteUniParcCrossReferenceStore(
                parameter.getNumberOfConnections(),
                parameter.isBrotliEnabled(),
                parameter.getBrotliLevel(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

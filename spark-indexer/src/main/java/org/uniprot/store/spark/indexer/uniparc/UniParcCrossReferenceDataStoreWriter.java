package org.uniprot.store.spark.indexer.uniparc;

import java.io.Serial;
import java.util.List;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.util.Pair;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UniParcCrossReferenceDataStoreWriter
        extends AbstractDataStoreWriter<Pair<String, List<UniParcCrossReference>>> {
    @Serial private static final long serialVersionUID = -5970975427885850402L;

    public UniParcCrossReferenceDataStoreWriter(DataStoreParameter parameter) {
        super(parameter);
    }

    @Override
    protected VoldemortClient<Pair<String, List<UniParcCrossReference>>> getDataStoreClient() {
        return new VoldemortRemoteUniParcCrossReferenceStore(
                parameter.getNumberOfConnections(),
                parameter.isBrotliEnabled(),
                parameter.getBrotliLevel(),
                parameter.getStoreName(),
                parameter.getConnectionURL());
    }
}

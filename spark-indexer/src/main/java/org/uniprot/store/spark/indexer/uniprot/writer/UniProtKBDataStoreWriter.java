package org.uniprot.store.spark.indexer.uniprot.writer;

import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;
import org.uniprot.store.spark.indexer.common.writer.AbstractDataStoreWriter;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
public class UniProtKBDataStoreWriter extends AbstractDataStoreWriter<UniProtKBEntry> {

    private static final long serialVersionUID = -7667879045351247007L;

    public UniProtKBDataStoreWriter(
            String numberOfConnections, String storeName, String connectionURL) {
        super(numberOfConnections, storeName, connectionURL);
    }

    @Override
    protected VoldemortClient<UniProtKBEntry> getDataStoreClient() {
        return new VoldemortRemoteUniProtKBEntryStore(
                numberOfConnections, storeName, connectionURL);
    }
}

package org.uniprot.store.spark.indexer.common.store;

import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniparc.UniParcDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefLightDataStoreIndexer;

/**
 * @author lgonzales
 * @since 27/04/2020
 */
public class DataStoreIndexerFactory {

    public DataStoreIndexer createDataStoreIndexer(DataStore dataStore, JobParameter jobParameter) {
        DataStoreIndexer result;
        switch (dataStore) {
            case UNIPROT:
                result = new UniProtKBDataStoreIndexer(jobParameter);
                break;
            case UNIREF:
                result = new UniRefDataStoreIndexer(jobParameter);
                break;
            case UNIREF_LIGHT:
                result = new UniRefLightDataStoreIndexer(jobParameter);
                break;
            case UNIPARC:
                result = new UniParcDataStoreIndexer(jobParameter);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Data Store '"
                                + dataStore.getName()
                                + "' not yet supported by spark indexer");
        }
        return result;
    }
}

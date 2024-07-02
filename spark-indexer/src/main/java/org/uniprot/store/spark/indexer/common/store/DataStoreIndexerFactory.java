package org.uniprot.store.spark.indexer.common.store;

import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniparc.UniParcCrossReferenceDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniparc.UniParcDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniparc.UniParcLightDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefLightDataStoreIndexer;
import org.uniprot.store.spark.indexer.uniref.UniRefMembersDataStoreIndexer;

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
            case UNIREF_LIGHT:
                result = new UniRefLightDataStoreIndexer(jobParameter);
                break;
            case UNIPARC:
                result = new UniParcDataStoreIndexer(jobParameter);
                break;
            case UNIPARC_LIGHT:
                result = new UniParcLightDataStoreIndexer(jobParameter);
                break;
            case UNIREF_MEMBER:
                result = new UniRefMembersDataStoreIndexer(jobParameter);
                break;
            case CROSS_REFERENCE:
                result = new UniParcCrossReferenceDataStoreIndexer(jobParameter);
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

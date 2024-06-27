package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class UniParcDataStoreIndexer extends BaseUniParcDataStoreIndexer<UniParcEntry> {

    private final JobParameter parameter;

    public UniParcDataStoreIndexer(JobParameter parameter) {
        super(parameter);
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        JavaRDD<UniParcEntry> uniParcJoinedRDD = getUniParcRDD();
        saveInDataStore(uniParcJoinedRDD);
        log.info("Completed UniParc Data Store index");
    }

    @Override
    void saveInDataStore(JavaRDD<UniParcEntry> uniParcJoinedRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcJoinedRDD.foreachPartition(new UniParcDataStoreWriter(dataStoreParameter));
    }
}

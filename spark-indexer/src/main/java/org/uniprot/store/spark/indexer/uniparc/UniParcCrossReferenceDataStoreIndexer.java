package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcCrossReferenceToWrapper;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UniParcCrossReferenceDataStoreIndexer extends BaseUniParcDataStoreIndexer {

    private final JobParameter parameter;

    public UniParcCrossReferenceDataStoreIndexer(JobParameter parameter) {
        super(parameter);
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        JavaRDD<UniParcEntry> uniParcRDD = getUniParcRDD();

        // <xrefIdUniqueKey, xrefObj>
        JavaRDD<UniParcCrossReferenceWrapper> crossRefIdCrossRef =
                uniParcRDD.flatMap(new UniParcCrossReferenceToWrapper());
        saveInDataStore(crossRefIdCrossRef);
        log.info("Completed UniParc Cross Reference Data Store index");
    }

    void saveInDataStore(JavaRDD<UniParcCrossReferenceWrapper> uniParcXrefRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcXrefRDD.foreachPartition(
                new UniParcCrossReferenceDataStoreWriter(dataStoreParameter));
    }

    @Override
    DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.cross-reference.numberOfConnections");
        String maxRetry = config.getString("store.cross-reference.retry");
        String delay = config.getString("store.cross-reference.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.cross-reference.host"))
                .storeName(config.getString("store.cross-reference.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}
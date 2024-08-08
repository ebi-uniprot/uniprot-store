package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcCrossReferenceMapper;

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
        Config config = parameter.getApplicationConfig();
        int xrefBatchSize = config.getInt("store.uniparc.cross.reference.batchSize");
        // <xrefIdUniqueKey, List<UniParcCrossReference>>
        JavaRDD<UniParcCrossReferencePair> crossRefIdCrossRef =
                uniParcRDD.flatMap(new UniParcCrossReferenceMapper(xrefBatchSize));
        saveInDataStore(crossRefIdCrossRef);
        log.info("Completed UniParc Cross Reference Data Store index");
    }

    void saveInDataStore(JavaRDD<UniParcCrossReferencePair> uniParcXrefRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcXrefRDD.foreachPartition(
                new UniParcCrossReferenceDataStoreWriter(dataStoreParameter));
    }

    @Override
    DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.uniparc.cross.reference.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.cross.reference.retry");
        String delay = config.getString("store.uniparc.cross.reference.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.cross.reference.host"))
                .storeName(config.getString("store.uniparc.cross.reference.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}

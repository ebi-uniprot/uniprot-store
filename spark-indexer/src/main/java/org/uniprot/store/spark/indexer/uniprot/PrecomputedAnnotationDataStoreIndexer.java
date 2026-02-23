package org.uniprot.store.spark.indexer.uniprot;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrecomputedAnnotationDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public PrecomputedAnnotationDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        // load the aa generated uniprotkb like entries
        PrecomputedAnnotationRDDReader precomputedAnnotationRDDReader =
                new PrecomputedAnnotationRDDReader(parameter);
        JavaRDD<UniProtKBEntry> uniProtKBEntryRDD = precomputedAnnotationRDDReader.load();
        saveInDataStore(uniProtKBEntryRDD);
        log.info("Completed UniProtKb Data Store index");
    }

    void saveInDataStore(JavaRDD<UniProtKBEntry> uniProtKBEntryRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniProtKBEntryRDD.foreachPartition(new UniProtKBDataStoreWriter(dataStoreParameter));
    }

    DataStoreParameter getDataStoreParameter(Config config) { // TODO create these configs
        String numberOfConnections =
                config.getString("store.precomputed.annotation.numberOfConnections");
        String maxRetry = config.getString("store.precomputed.annotation.retry");
        String delay = config.getString("store.precomputed.annotation.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.precomputed.annotation.host"))
                .storeName(config.getString("store.precomputed.annotation.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}

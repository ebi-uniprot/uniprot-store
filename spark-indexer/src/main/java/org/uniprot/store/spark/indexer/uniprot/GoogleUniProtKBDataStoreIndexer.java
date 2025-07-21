package org.uniprot.store.spark.indexer.uniprot;

import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleUniProtKBDataStoreIndexer implements DataStoreIndexer {
    private final JobParameter parameter;

    public GoogleUniProtKBDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        GoogleUniProtKBRDDReader reader = new GoogleUniProtKBRDDReader(this.parameter);
        JavaRDD<UniProtKBEntry> uniProtKBRDD = reader.load();
        Config config = this.parameter.getApplicationConfig();
        DataStoreParameter storeParams = getDataStoreParameter(config);
        log.info("Writing google protlm entries to datastore...");
        uniProtKBRDD.foreachPartition(getDataStoreWriter(storeParams));
        log.info("Completed writing google protlm entries to datastore...");
    }

    private DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.google.protlm.numberOfConnections");
        String maxRetry = config.getString("store.google.protlm.retry");
        String delay = config.getString("store.google.protlm.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.google.protlm.host"))
                .storeName(config.getString("store.google.protlm.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }

    VoidFunction<Iterator<UniProtKBEntry>> getDataStoreWriter(DataStoreParameter parameter) {
        return new UniProtKBDataStoreWriter(parameter);
    }
}

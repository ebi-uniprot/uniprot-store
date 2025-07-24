package org.uniprot.store.spark.indexer.uniprot;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleProtLMEntryUpdater;
import org.uniprot.store.spark.indexer.uniprot.writer.UniProtKBDataStoreWriter;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class GoogleUniProtKBDataStoreIndexer implements DataStoreIndexer {
    private final JobParameter parameter;

    public GoogleUniProtKBDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        GoogleUniProtKBRDDReader protLMReader = new GoogleUniProtKBRDDReader(this.parameter);
        // accession,protlmentry(uniprtkb) entry pair
        JavaPairRDD<String, UniProtKBEntry> protLMPairRDDPair = protLMReader.load();
        // read trembl entry to join
        UniProtKBRDDTupleReader uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniProtRDDPair = uniProtKBReader.load();
        // join protlmentry with uniprotkbentry and inject proteinId in protlm entry
        JavaRDD<UniProtKBEntry> protLMRDD = joinRDDPairs(protLMPairRDDPair, uniProtRDDPair);
        log.info("Writing google protlm entries to datastore...");
        saveInDataStore(protLMRDD);
        log.info("Completed writing google protlm entries to datastore...");
    }

    JavaRDD<UniProtKBEntry> joinRDDPairs(
            JavaPairRDD<String, UniProtKBEntry> protLMPairRDD,
            JavaPairRDD<String, UniProtKBEntry> uniProtRDDPair) {
        JavaSparkContext jsc = parameter.getSparkContext();
        Map<String, UniProtKBEntry> protLMMap = protLMPairRDD.collectAsMap();
        Broadcast<Map<String, UniProtKBEntry>> broadcastProtLMMap = jsc.broadcast(protLMMap);

        return uniProtRDDPair
                .filter(t -> broadcastProtLMMap.value().containsKey(t._1))
                .map(
                        t -> {
                            String key = t._1;
                            UniProtKBEntry uniProtEntry = t._2;
                            UniProtKBEntry protLMEntry = broadcastProtLMMap.value().get(key);
                            return new GoogleProtLMEntryUpdater()
                                    .call(new Tuple2<>(protLMEntry, uniProtEntry));
                        });
    }

    void saveInDataStore(JavaRDD<UniProtKBEntry> protLMEntryRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        protLMEntryRDD.foreachPartition(new UniProtKBDataStoreWriter(dataStoreParameter));
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
}

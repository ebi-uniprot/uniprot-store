package org.uniprot.store.spark.indexer.uniprot;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.keyword.KeywordRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoogleProtNLMEntryUpdater;
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
        GoogleUniProtKBRDDReader protNLMReader = new GoogleUniProtKBRDDReader(this.parameter);
        // accession,protnlmentry(uniprtkb) entry pair
        JavaPairRDD<String, UniProtKBEntry> protNLMPairRDDPair = protNLMReader.load();
        // read trembl entry to join
        UniProtKBRDDTupleReader uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniProtRDDPair = uniProtKBReader.load();
        // join protnlmentry with uniprotkbentry and inject proteinId in protnlm entry
        JavaRDD<UniProtKBEntry> protNLMRDD = joinRDDPairs(protNLMPairRDDPair, uniProtRDDPair);

        log.info("Writing google protnlm entries to datastore...");
        saveInDataStore(protNLMRDD);
        log.info("Completed writing google protnlm entries to datastore...");
    }

    JavaRDD<UniProtKBEntry> joinRDDPairs(
            JavaPairRDD<String, UniProtKBEntry> protNLMPairRDD,
            JavaPairRDD<String, UniProtKBEntry> uniProtRDDPair) {
        JavaSparkContext jsc = parameter.getSparkContext();
        Map<String, UniProtKBEntry> protNLMMap = protNLMPairRDD.collectAsMap();
        Broadcast<Map<String, UniProtKBEntry>> broadcastProtNLMMap = jsc.broadcast(protNLMMap);

        // JavaPairRDD<keywordId,KeywordEntry> keyword --> extracted from keywlist.txt
        KeywordRDDReader keywordReader = new KeywordRDDReader(this.parameter);
        JavaPairRDD<String, KeywordEntry> keyword = keywordReader.load();
        Map<String, KeywordEntry> keywordAccEntryMap = new HashMap<>(keyword.collectAsMap());
        log.info("################### Writing keyword entries to datastore... {}", keywordAccEntryMap.size());
        Broadcast<Map<String, KeywordEntry>> broadcastKeywordAccEntryMap = jsc.broadcast(keywordAccEntryMap);

        return uniProtRDDPair
                .filter(t -> broadcastProtNLMMap.value().containsKey(t._1))
                .map(
                        t -> {
                            String key = t._1;
                            UniProtKBEntry uniProtEntry = t._2;
                            UniProtKBEntry protNLMEntry = broadcastProtNLMMap.value().get(key);
                            return new GoogleProtNLMEntryUpdater(broadcastKeywordAccEntryMap.value())
                                    .call(new Tuple2<>(protNLMEntry, uniProtEntry));
                        });
    }

    void saveInDataStore(JavaRDD<UniProtKBEntry> protNLMEntryRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        protNLMEntryRDD.foreachPartition(new UniProtKBDataStoreWriter(dataStoreParameter));
    }

    private DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.google.protnlm.numberOfConnections");
        String maxRetry = config.getString("store.google.protnlm.retry");
        String delay = config.getString("store.google.protnlm.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.google.protnlm.host"))
                .storeName(config.getString("store.google.protnlm.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}

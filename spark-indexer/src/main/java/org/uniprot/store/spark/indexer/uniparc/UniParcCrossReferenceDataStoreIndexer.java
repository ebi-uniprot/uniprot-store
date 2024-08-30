package org.uniprot.store.spark.indexer.uniparc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParSourceJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcCrossReferenceMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcSequenceSourceJoin;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBUniParcMappingRDDTupleReader;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

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
                uniParcRDD
                        .mapToPair(e -> new Tuple2<>(e.getUniParcId().getValue(), e))
                        .leftOuterJoin(loadSequenceSource())
                        .mapValues(new UniParcSequenceSourceJoin())
                        .values()
                        .flatMap(new UniParcCrossReferenceMapper(xrefBatchSize));
        saveInDataStore(crossRefIdCrossRef);
        log.info("Completed UniParc Cross Reference Data Store index");
    }

    JavaPairRDD<String, Map<String, Set<String>>> loadSequenceSource() {
        UniProtKBUniParcMappingRDDTupleReader uniProtKBUniParcMapper =
                new UniProtKBUniParcMappingRDDTupleReader(parameter);
        // JavaPairRDD<accession, UniParcId>
        JavaPairRDD<String, String> uniParcJoinRdd = uniProtKBUniParcMapper.load();

        UniParcSequenceSourceMapperRDDTupleReader sourceMapper =
                new UniParcSequenceSourceMapperRDDTupleReader(parameter);
        // JavaPairRDD<accession, Set<sourceIds>>
        JavaPairRDD<String, Set<String>> sourceRDD = sourceMapper.load();

        return uniParcJoinRdd
                .leftOuterJoin(sourceRDD)
                .mapToPair(new UniParSourceJoin())
                .aggregateByKey(new HashMap<>(), aggregate(), aggregate());
    }

    private Function2<Map<String, Set<String>>, Map<String, Set<String>>, Map<String, Set<String>>>
            aggregate() {
        return (s1, s2) -> {
            s1.putAll(s2);
            return s1;
        };
    }

    void saveInDataStore(JavaRDD<UniParcCrossReferencePair> uniParcXrefRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcXrefRDD.foreachPartition(
                new UniParcCrossReferenceDataStoreWriter(dataStoreParameter));
    }

    @Override
    DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections =
                config.getString("store.uniparc.cross.reference.numberOfConnections");
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

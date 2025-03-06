package org.uniprot.store.spark.indexer.uniparc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.*;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcCrossReferenceMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryKeyMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBUniParcMappingRDDTupleReader;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UniParcCrossReferenceDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniParcCrossReferenceDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        Config config = parameter.getApplicationConfig();
        int xrefBatchSize = config.getInt("store.uniparc.cross.reference.batchSize");

        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();

        // JavaPairRDD<uniParcId,Iterable<TaxonomyEntry>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniParcTaxonomyJoin =
                getUniParcTaxonomyRDD(uniParcRDD);

        // JavaPairRDD<uniParcId, Map<source, Set<accession>>>
        JavaPairRDD<String, Map<String, Set<String>>> sequenceSourceRDD = loadSequenceSource();

        // JavaPairRDD<uniParcId, UniParcTaxonomySequenceSource>
        JavaPairRDD<String, UniParcTaxonomySequenceSource> uniParcTaxonomySourceRDD =
                uniParcTaxonomyJoin
                        .fullOuterJoin(sequenceSourceRDD)
                        .mapValues(new UniParcTaxonomySequenceSourceJoin());

        // <xrefIdUniqueKey, List<UniParcCrossReference>>
        JavaRDD<UniParcCrossReferencePair> crossRefIdCrossRef =
                uniParcRDD
                        .mapToPair(new UniParcEntryKeyMapper())
                        .leftOuterJoin(uniParcTaxonomySourceRDD)
                        .map(new UniParcEntryJoin())
                        .flatMap(new UniParcCrossReferenceMapper(xrefBatchSize));

        saveInDataStore(crossRefIdCrossRef);
        log.info("Completed UniParc Cross Reference Data Store index");
    }

    /**
     * @return JavaPairRDD<UniParcID, Map<accession, Set<sources>>>
     */
    JavaPairRDD<String, Map<String, Set<String>>> loadSequenceSource() {
        UniProtKBUniParcMappingRDDTupleReader uniProtKBUniParcMapper =
                new UniProtKBUniParcMappingRDDTupleReader(parameter);
        // JavaPairRDD<accession, UniParcId>
        JavaPairRDD<String, String> uniProtUniParcRDD = uniProtKBUniParcMapper.load();

        UniParcSequenceSourceMapperRDDTupleReader sourceMapper =
                new UniParcSequenceSourceMapperRDDTupleReader(parameter);
        // JavaPairRDD<accession, Set<sourceIds>>
        JavaPairRDD<String, Set<String>> sourceRDD = sourceMapper.load();

        return uniProtUniParcRDD
                .leftOuterJoin(sourceRDD)
                .mapToPair(new UniProtUniParcSourceJoin())
                .aggregateByKey(new HashMap<>(), aggregate(), aggregate());
    }

    private Function2<Map<String, Set<String>>, Map<String, Set<String>>, Map<String, Set<String>>>
            aggregate() {
        return (s1, s2) -> {
            Map<String, Set<String>> mergedMap = new HashMap<>(s1);
            mergedMap.putAll(s2);
            return mergedMap;
        };
    }

    void saveInDataStore(JavaRDD<UniParcCrossReferencePair> uniParcXrefRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcXrefRDD.foreachPartition(
                new UniParcCrossReferenceDataStoreWriter(dataStoreParameter));
    }

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

    protected JavaPairRDD<String, Iterable<TaxonomyEntry>> getUniParcTaxonomyRDD(
            JavaRDD<UniParcEntry> uniParcRDD) {
        // JavaPairRDD<taxId,uniParcId>
        JavaPairRDD<String, String> taxonomyJoin =
                uniParcRDD.flatMapToPair(new UniParcTaxonomyMapper());

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                loadTaxonomyEntryJavaPairRDD();

        // JavaPairRDD<uniParcId,Iterable<TaxonomyEntry>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniParcJoin =
                taxonomyJoin
                        .join(taxonomyEntryJavaPairRDD)
                        // After Join RDD: JavaPairRDD<taxId,Tuple2<uniParcId,TaxonomyEntry>>
                        .mapToPair(tuple -> tuple._2)
                        .groupByKey();

        return uniParcJoin;
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, false);
        return taxReader.load();
    }
}

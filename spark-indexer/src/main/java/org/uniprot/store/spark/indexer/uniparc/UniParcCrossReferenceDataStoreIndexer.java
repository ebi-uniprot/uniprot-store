package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.store.DataStoreIndexer.BROTLI_COMPRESSION_ENABLED;
import static org.uniprot.store.spark.indexer.common.store.DataStoreIndexer.BROTLI_COMPRESSION_LEVEL;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcCrossReferenceMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryKeyMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;

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

    protected JavaRDD<UniParcEntry> getUniParcRDD() {
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniParcRDD = reader.load();

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

        return uniParcRDD
                .mapToPair(new UniParcEntryKeyMapper())
                .leftOuterJoin(uniParcJoin)
                .map(new UniParcEntryTaxonomyJoin());
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, false);
        return taxReader.load();
    }
}

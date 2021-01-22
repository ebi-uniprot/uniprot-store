package org.uniprot.store.spark.indexer.uniparc;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryKeyMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;

/**
 * @author lgonzales
 * @since 2020-02-26
 */
@Slf4j
public class UniParcDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniParcDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntry> uniparcRDD = reader.load();

        // JavaPairRDD<taxId,uniparcId>
        JavaPairRDD<String, String> taxonomyJoin =
                uniparcRDD.flatMapToPair(new UniParcTaxonomyMapper());

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                loadTaxonomyEntryJavaPairRDD();

        // JavaPairRDD<uniparcId,Iterable<Taxonomy with lineage>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniparcJoin =
                taxonomyJoin
                        .join(taxonomyEntryJavaPairRDD)
                        // After Join RDD: JavaPairRDD<taxId,Tuple2<uniparcId,TaxonomyEntry>>
                        .mapToPair(tuple -> tuple._2)
                        .groupByKey();

        JavaRDD<UniParcEntry> uniparcJoinedRDD =
                uniparcRDD
                        .mapToPair(new UniParcEntryKeyMapper())
                        .leftOuterJoin(uniparcJoin)
                        .map(new UniParcEntryTaxonomyJoin());

        saveInDataStore(uniparcJoinedRDD);

        log.info("Completed UniParc Data Store index");
    }

    void saveInDataStore(JavaRDD<UniParcEntry> uniparcJoinedRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniparcJoinedRDD.foreachPartition(new UniParcDataStoreWriter(dataStoreParameter));
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, false);
        return taxReader.load();
    }

    private DataStoreParameter getDataStoreParameter(ResourceBundle config) {
        String numberOfConnections = config.getString("store.uniparc.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.retry");
        String delay = config.getString("store.uniparc.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.host"))
                .storeName(config.getString("store.uniparc.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .build();
    }
}

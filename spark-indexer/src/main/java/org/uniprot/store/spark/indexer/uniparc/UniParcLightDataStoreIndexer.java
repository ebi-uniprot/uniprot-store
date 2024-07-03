package org.uniprot.store.spark.indexer.uniparc;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyLineageReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.*;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class UniParcLightDataStoreIndexer implements DataStoreIndexer {

    private final JobParameter parameter;

    public UniParcLightDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void indexInDataStore() {
        // load uniparc file
        UniParcLightRDDTupleReader reader = new UniParcLightRDDTupleReader(parameter, false);
        JavaRDD<UniParcEntryLight> uniParcLightRDD = reader.load();

        // JavaPairRDD<taxId,uniParcId>
        JavaPairRDD<String, String> taxonomyJoin =
                uniParcLightRDD.flatMapToPair(new UniParcLightTaxonomyMapper());

        // JavaPairRDD<taxId,List<grandparent of taxId, parent of taxId, taxId>>
        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyWithLineage =
                getTaxonomyWithLineageRDD();

        // inner join taxonomyJoin with taxonomyWithLineage
        JavaPairRDD<String, List<List<TaxonomyLineage>>> uniParcIdTaxonLineages =
                taxonomyJoin
                        .join(taxonomyWithLineage)
                        .mapToPair(tuple -> tuple._2)
                        .aggregateByKey(
                                new ArrayList<>(),
                                new TaxonomyLineageToAccumulator(),
                                new TaxonomyLineageAccumulatorsMerger());

        // uniParc id and List of Tuples<toplevel organism, commonTaxon>
        // e.g. <UPI000001, [<"cellular organisms", "Bacteria">, <"Viruses", "Zilligvirae">]>
        JavaPairRDD<String, List<Tuple2<String, String>>> uniParcIdCommonTaxons =
                uniParcIdTaxonLineages.mapToPair(new TaxonomyCommonalityAggregator());
        // convert uniParcLightRDD to <uniParcId, uniParcLight> and then join with
        // uniParcIdTaxonLineages.
        // then map to inject common taxons in uniParcLightRDD
        uniParcLightRDD =
                uniParcLightRDD
                        .mapToPair(uniParc -> new Tuple2<>(uniParc.getUniParcId(), uniParc))
                        .join(uniParcIdCommonTaxons)
                        .mapValues(new UniParcEntryLightTaxonMapper())
                        .values();

        saveInDataStore(uniParcLightRDD);
        log.info("Completed UniParc Light Data Store index");
    }

    JavaPairRDD<String, List<TaxonomyLineage>> getTaxonomyWithLineageRDD() {
        // compute the lineage of the taxonomy ids in the format <2, <1,1315,2>> using db
        TaxonomyLineageReader lineageReader = new TaxonomyLineageReader(parameter, true);
        JavaPairRDD<String, List<TaxonomyLineage>> taxonomyWithLineage = lineageReader.load();
        taxonomyWithLineage.repartition(taxonomyWithLineage.getNumPartitions());
        taxonomyWithLineage.persist(StorageLevel.DISK_ONLY());
        // Call terminal operator count to have this RDD persisted on disk
        log.info("Total number of TaxonomyLineage : " + taxonomyWithLineage.count());
        return taxonomyWithLineage;
    }

    void saveInDataStore(JavaRDD<UniParcEntryLight> uniParcJoinedRDD) {
        DataStoreParameter dataStoreParameter =
                getDataStoreParameter(parameter.getApplicationConfig());
        uniParcJoinedRDD.foreachPartition(new UniParcLightDataStoreWriter(dataStoreParameter));
    }

    DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.uniparc.light.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.light.retry");
        String delay = config.getString("store.uniparc.light.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.light.host"))
                .storeName(config.getString("store.uniparc.light.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}

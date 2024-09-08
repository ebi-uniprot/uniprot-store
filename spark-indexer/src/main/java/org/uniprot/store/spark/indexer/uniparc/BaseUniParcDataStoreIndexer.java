package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreIndexer;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryKeyMapper;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;

import com.typesafe.config.Config;

public abstract class BaseUniParcDataStoreIndexer implements DataStoreIndexer {
    private final JobParameter parameter;

    BaseUniParcDataStoreIndexer(JobParameter parameter) {
        this.parameter = parameter;
    }

    /**
     * We load UniParc Data from XML, join it with Taxonomy data to inject Scientific Name and
     * Common name, and save it to DataStore.
     *
     * <p>To join Taxonomy we are creating an RDD of JavaPairRDD<taxId,uniParcId> extracted from
     * UniParc RDD. One UniParc Entry usually has many taxIds. For Example: Tuple2<9606,UP00000001>
     * ad Tuple2<1106,UP00000001>
     *
     * <p>The Second step we load all taxonomy data to an RDD, JavaPairRDD<taxId,TaxonomyEntry>,
     * this RDD will be used to join.
     *
     * <p>The Third step is a join between JavaPairRDD<taxId,uniParcId> and
     * JavaPairRDD<taxId,TaxonomyEntry> we group by uniParcId, so the result RDD would be a
     * (JavaPairRDD<uniParcId, Iterable<TaxonomyEntry>>). For example, the UniParc UP00000001 would
     * have one tuple in the RDD: Tuple2<UP00000001,
     * Iterable<TaxonomyEntry(9606),TaxonomyEntry(1106)>>
     *
     * <p>The Fourth step is to join JavaPairRDD<uniParcId, Iterable<TaxonomyEntry>> with
     * JavaPairRDD<uniParcId, UniParcEntry> and at this point we can map TaxonomyEntry information
     * into UniParcEntry.
     *
     * <p>The Fifth and last step is to save our UniParcEntry into our DataStore.
     */
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

/*        JavaRDD<UniParcEntry> uniParcJoinedRDD =
                uniParcRDD
                        .mapToPair(new UniParcEntryKeyMapper())
                        .leftOuterJoin(uniParcJoin)
                        .map(new UniParcEntryJoin());*/
        return uniParcRDD;
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, false);
        return taxReader.load();
    }

    DataStoreParameter getDataStoreParameter(Config config) {
        String numberOfConnections = config.getString("store.uniparc.numberOfConnections");
        String maxRetry = config.getString("store.uniparc.retry");
        String delay = config.getString("store.uniparc.delay");
        return DataStoreParameter.builder()
                .connectionURL(config.getString("store.uniparc.host"))
                .storeName(config.getString("store.uniparc.storeName"))
                .numberOfConnections(Integer.parseInt(numberOfConnections))
                .maxRetry(Integer.parseInt(maxRetry))
                .delay(Long.parseLong(delay))
                .brotliEnabled(config.getBoolean(BROTLI_COMPRESSION_ENABLED))
                .brotliLevel(config.getInt(BROTLI_COMPRESSION_LEVEL))
                .build();
    }
}

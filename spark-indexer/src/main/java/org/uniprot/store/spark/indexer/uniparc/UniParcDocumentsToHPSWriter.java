package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcDocTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryToDocument;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.util.Map;

/**
 * This class is responsible to load all the data for UniParcDocument and save it into HPS
 *
 * @author lgonzales
 * @since 2020-02-13
 */
@Slf4j
public class UniParcDocumentsToHPSWriter implements DocumentsToHPSWriter {

    private final JobParameter parameter;
    private final Config config;
    private final String releaseName;

    public UniParcDocumentsToHPSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    /** load all the data for UniParcDocument and write it into HPS (Hadoop File System) */
    //@Override
    public void writeIndexDocumentsToHPS_old() {
        UniParcRDDTupleReader uniparcReader = new UniParcRDDTupleReader(parameter, true);
        JavaRDD<UniParcEntry> uniparcRDD = uniparcReader.load();

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                loadTaxonomyEntryJavaPairRDD();

        // JavaPairRDD<taxId,uniparcId>
        JavaPairRDD<String, String> taxonomyJoin =
                uniparcRDD.flatMapToPair(new UniParcTaxonomyMapper());

        // JavaPairRDD<uniparcId,Iterable<Taxonomy with lineage>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniparcJoin =
                taxonomyJoin
                        .join(taxonomyEntryJavaPairRDD)
                        // After Join RDD: JavaPairRDD<taxId,Tuple2<uniparcId,TaxonomyEntry>>
                        .mapToPair(tuple -> tuple._2)
                        .groupByKey();

        // JavaPairRDD<uniparcId,UniParcDocument>
        JavaPairRDD<String, UniParcDocument> uniparcDocumentRDD =
                uniparcRDD.mapToPair(new UniParcEntryToDocument());

        JavaRDD<UniParcDocument> uniParcDocumentRDD =
                uniparcDocumentRDD
                        .leftOuterJoin(uniparcJoin)
                        // After Join RDD:
                        // JavaPairRDD<uniparcId,Tuple2<UniParcDocument,Iterable<TaxonomyEntry>>>
                        .mapValues(new UniParcDocTaxonomyJoin())
                        .values();

        saveToHPS(uniParcDocumentRDD);

        log.info("Completed UniParc prepare Solr index");
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        UniParcRDDTupleReader uniparcReader = new UniParcRDDTupleReader(parameter, true);
        JavaRDD<UniParcEntry> uniparcRDD = uniparcReader.load().persist(StorageLevel.MEMORY_AND_DISK());

        // JavaPairRDD<taxId, uniparcId>
        JavaPairRDD<String, String> taxonomyUniparcJoin =
                uniparcRDD.flatMapToPair(new UniParcTaxonomyMapper())
                        .filter(tuple -> tuple._1 != null)          // avoid null keys
                        .repartition(/*numPartitionsForJoins()*/72);      // reduce shuffle cost

        // Broadcast taxonomy dimension RDD (small dataset)
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryRDD =
                loadTaxonomyEntryJavaPairRDD().persist(StorageLevel.MEMORY_ONLY());

        Map<String, TaxonomyEntry> taxonomyMap = taxonomyEntryRDD.collectAsMap();
        Broadcast<Map<String, TaxonomyEntry>> taxonomyBC = parameter.getSparkContext().broadcast(taxonomyMap);

        // JavaPairRDD<uniparcId, Iterable<Taxonomy with lineage>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniparcTaxLineageJoin =
                taxonomyUniparcJoin
                        .mapToPair(t -> {
                            TaxonomyEntry te = taxonomyBC.value().get(t._1);
                            return new Tuple2<>(t._2, te);
                        })
                        .filter(t -> t._2 != null)
                        .groupByKey(/*numPartitionsForAggregation()*/24000); // avoids wide shuffle

        // JavaPairRDD<uniparcId, UniParcDocument>
        JavaPairRDD<String, UniParcDocument> uniparcDocumentRDD =
                uniparcRDD
                        .mapToPair(new UniParcEntryToDocument())
                        .persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<UniParcDocument> uniParcDocumentRDD =
                uniparcDocumentRDD
                        .leftOuterJoin(uniparcTaxLineageJoin)
                        .mapValues(new UniParcDocTaxonomyJoin())
                        .values();

        saveToHPS(uniParcDocumentRDD);

        log.info("Completed UniParc prepare Solr index");

        // Cleanup to free memory
        uniparcRDD.unpersist();
        taxonomyEntryRDD.unpersist();
        uniparcDocumentRDD.unpersist();
    }


    void saveToHPS(JavaRDD<UniParcDocument> uniParcDocumentRDD) {
        String hpsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.uniparc);
        SolrUtils.saveSolrInputDocumentRDD(uniParcDocumentRDD, hpsPath);
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, true);
        return taxReader.load();
    }
}

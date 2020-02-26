package org.uniprot.store.spark.indexer.uniparc;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryToDocument;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;
import org.uniprot.store.spark.indexer.util.SolrUtils;

/**
 * This class is responsible to load all the data for UniParcDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2020-02-13
 */
@Slf4j
public class UniParcIndexer {

    public static void writeIndexDocumentsToHDFS(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        SparkConf sparkConf = sparkContext.sc().conf();

        JavaRDD<UniParcEntry> uniparcRDD =
                (JavaRDD<UniParcEntry>) UniParcRDDTupleReader.load(sparkConf, applicationConfig);

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                TaxonomyRDDReader.loadWithLineage(sparkContext, applicationConfig);

        // JavaPairRDD<taxId,uniparcId>
        JavaPairRDD<String, String> taxonomyJoin =
                (JavaPairRDD<String, String>) uniparcRDD.flatMapToPair(new UniParcTaxonomyMapper());

        // JavaPairRDD<uniparcId,Iterable<Taxonomy with lineage>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> uniparcJoin =
                taxonomyJoin
                        .join(taxonomyEntryJavaPairRDD)
                        // After Join RDD: JavaPairRDD<taxId,Tuple2<uniparcId,TaxonomyEntry>>
                        .mapToPair(tuple -> tuple._2)
                        .groupByKey();

        // JavaPairRDD<uniparcId,UniParcDocument>
        JavaPairRDD<String, UniParcDocument> uniparcDocumentRDD =
                (JavaPairRDD<String, UniParcDocument>)
                        uniparcRDD.mapToPair(new UniParcEntryToDocument());

        JavaRDD<UniParcDocument> uniParcDocumentRDD =
                uniparcDocumentRDD
                        .leftOuterJoin(uniparcJoin)
                        // After Join RDD:
                        // JavaPairRDD<uniparcId,Tuple2<UniParcDocument,Iterable<TaxonomyEntry>>>
                        .mapValues(new UniParcTaxonomyJoin())
                        .values();

        String hdfsPath = applicationConfig.getString("uniparc.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(uniParcDocumentRDD, hdfsPath);

        log.info("Completed UniParc prepare Solr index");
    }
}

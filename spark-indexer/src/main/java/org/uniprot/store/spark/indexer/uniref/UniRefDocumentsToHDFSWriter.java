package org.uniprot.store.spark.indexer.uniref;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefToDocument;
import org.uniprot.store.spark.indexer.util.SolrUtils;

/**
 * This class is responsible to load all the data for UniRefDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2020-02-07
 */
@Slf4j
public class UniRefDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final ResourceBundle applicationConfig;

    public UniRefDocumentsToHDFSWriter(ResourceBundle applicationConfig) {
        this.applicationConfig = applicationConfig;
    }

    @Override
    public void writeIndexDocumentsToHDFS(JavaSparkContext sparkContext, String releaseName) {
        SparkConf sparkConf = sparkContext.sc().conf();

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                TaxonomyRDDReader.loadWithLineage(sparkContext, applicationConfig);

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref50DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef50,
                                        sparkConf,
                                        applicationConfig,
                                        releaseName)
                                .mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref90DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef90,
                                        sparkConf,
                                        applicationConfig,
                                        releaseName)
                                .mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref100DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef100,
                                        sparkConf,
                                        applicationConfig,
                                        releaseName)
                                .mapToPair(new UniRefToDocument());

        JavaRDD<UniRefDocument> unirefDocumentRDD =
                joinTaxonomy(uniref50DocRDD, taxonomyEntryJavaPairRDD)
                        .union(joinTaxonomy(uniref90DocRDD, taxonomyEntryJavaPairRDD))
                        .union(joinTaxonomy(uniref100DocRDD, taxonomyEntryJavaPairRDD));

        String hdfsPath = applicationConfig.getString("uniref.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(unirefDocumentRDD, hdfsPath);

        log.info("Completed UniRef (100, 90 and 50) prepare Solr index");
    }

    private static JavaRDD<UniRefDocument> joinTaxonomy(
            JavaPairRDD<String, UniRefDocument> uniRefRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        return uniRefRDD.leftOuterJoin(taxonomyRDD).mapValues(new UniRefTaxonomyJoin()).values();
    }
}

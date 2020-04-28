package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefToDocument;

/**
 * This class is responsible to load all the data for UniRefDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2020-02-07
 */
@Slf4j
public class UniRefDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final ResourceBundle config;
    private final JavaSparkContext sparkContext;
    private final String releaseName;

    public UniRefDocumentsToHDFSWriter(JobParameter parameter) {
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
        this.sparkContext = parameter.getSparkContext();
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        SparkConf sparkConf = sparkContext.sc().conf();

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                TaxonomyRDDReader.loadWithLineage(sparkContext, config);

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref50DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef50, sparkConf, config, releaseName)
                                .mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref90DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef90, sparkConf, config, releaseName)
                                .mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        JavaPairRDD<String, UniRefDocument> uniref100DocRDD =
                (JavaPairRDD<String, UniRefDocument>)
                        UniRefRDDTupleReader.load(
                                        UniRefType.UniRef100, sparkConf, config, releaseName)
                                .mapToPair(new UniRefToDocument());

        JavaRDD<UniRefDocument> unirefDocumentRDD =
                joinTaxonomy(uniref50DocRDD, taxonomyEntryJavaPairRDD)
                        .union(joinTaxonomy(uniref90DocRDD, taxonomyEntryJavaPairRDD))
                        .union(joinTaxonomy(uniref100DocRDD, taxonomyEntryJavaPairRDD));

        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.uniref);
        SolrUtils.saveSolrInputDocumentRDD(unirefDocumentRDD, hdfsPath);

        log.info("Completed UniRef (100, 90 and 50) prepare Solr index");
    }

    private static JavaRDD<UniRefDocument> joinTaxonomy(
            JavaPairRDD<String, UniRefDocument> uniRefRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        return uniRefRDD.leftOuterJoin(taxonomyRDD).mapValues(new UniRefTaxonomyJoin()).values();
    }
}

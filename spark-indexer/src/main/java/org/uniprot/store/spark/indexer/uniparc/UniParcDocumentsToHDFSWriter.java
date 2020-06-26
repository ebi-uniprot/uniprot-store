package org.uniprot.store.spark.indexer.uniparc;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcEntryToDocument;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniparc.mapper.UniParcTaxonomyMapper;

/**
 * This class is responsible to load all the data for UniParcDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2020-02-13
 */
@Slf4j
public class UniParcDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter parameter;
    private final ResourceBundle config;
    private final String releaseName;

    public UniParcDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    /** load all the data for UniParcDocument and write it into HDFS (Hadoop File System) */
    @Override
    public void writeIndexDocumentsToHDFS() {
        UniParcRDDTupleReader uniparcReader = new UniParcRDDTupleReader(parameter, true);
        JavaRDD<UniParcEntry> uniparcRDD = uniparcReader.load();

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD = loadTaxonomyEntryJavaPairRDD();

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
                        .mapValues(new UniParcTaxonomyJoin())
                        .values();

        saveToHDFS(uniParcDocumentRDD);

        log.info("Completed UniParc prepare Solr index");
    }

    void saveToHDFS(JavaRDD<UniParcDocument> uniParcDocumentRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.uniparc);
        SolrUtils.saveSolrInputDocumentRDD(uniParcDocumentRDD, hdfsPath);
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, true);
        return taxReader.load();
    }
}

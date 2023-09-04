package org.uniprot.store.spark.indexer.proteome;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeEntryToProteomeDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.StatisticsToProteomeDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.TaxonomyToProteomeDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.reader.ProteomeStatisticsReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;

import scala.Tuple2;

public class ProteomeDocumentsToHPSWriter implements DocumentsToHPSWriter {
    private final JobParameter jobParameter;
    private final ProteomeStatisticsReader proteomeStatisticsReader;
    private final TaxonomyRDDReader taxonomyRDDReader;
    private final ProteomeRDDReader proteomeRDDReader;

    public ProteomeDocumentsToHPSWriter(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.proteomeRDDReader = new ProteomeRDDReader(jobParameter, true);
        this.proteomeStatisticsReader = new ProteomeStatisticsReader(jobParameter);
        this.taxonomyRDDReader = new TaxonomyRDDReader(jobParameter, true);
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        JavaPairRDD<String, ProteomeDocument> idProteomeDocumentJavaPairRDD =
                loadProteomeRDD().mapValues(getEntryToProteomeDocumentMapper());

        idProteomeDocumentJavaPairRDD = joinStatistics(idProteomeDocumentJavaPairRDD);
        idProteomeDocumentJavaPairRDD = joinTaxonomy(idProteomeDocumentJavaPairRDD);

        saveToHPS(idProteomeDocumentJavaPairRDD.values());
    }

    private JavaPairRDD<String, ProteomeDocument> joinTaxonomy(
            JavaPairRDD<String, ProteomeDocument> proteomeIdProteomeDocumentJavaPairRDD) {
        // <organismId, proteomeId>
        JavaPairRDD<String, String> taxIdProteomeIdJavaRDD =
                JavaPairRDD.fromJavaRDD(
                        proteomeIdProteomeDocumentJavaPairRDD.map(
                                kv -> new Tuple2<>(String.valueOf(kv._2.organismTaxId), kv._1)));
        //<taxId, taxEntry>
        JavaPairRDD<String, TaxonomyEntry> taxIdTaxEntryRDD = getTaxonomyRDD();

        // <proteomeId, taxonEntry>
        JavaPairRDD<String, TaxonomyEntry> proteomeIdTaxEntryJavaRDD =
                JavaPairRDD.fromJavaRDD(taxIdProteomeIdJavaRDD.join(taxIdTaxEntryRDD).values());

        return proteomeIdProteomeDocumentJavaPairRDD
                .leftOuterJoin(proteomeIdTaxEntryJavaRDD)
                .mapValues(getTaxonomyToProteomeDocumentMapper());
    }

    private JavaPairRDD<String, ProteomeDocument> joinStatistics(
            JavaPairRDD<String, ProteomeDocument> idProteomeDocumentJavaPairRDD) {
        return idProteomeDocumentJavaPairRDD
                .leftOuterJoin(getProteomeStatisticsRDD())
                .mapValues(getStatisticsToProteomeDocumentMapper());
    }

    Function<Tuple2<ProteomeDocument, Optional<TaxonomyEntry>>, ProteomeDocument>
            getTaxonomyToProteomeDocumentMapper() {
        return new TaxonomyToProteomeDocumentMapper();
    }

    Function<Tuple2<ProteomeDocument, Optional<ProteomeStatistics>>, ProteomeDocument>
            getStatisticsToProteomeDocumentMapper() {
        return new StatisticsToProteomeDocumentMapper();
    }

    Function<ProteomeEntry, ProteomeDocument> getEntryToProteomeDocumentMapper() {
        return new ProteomeEntryToProteomeDocumentMapper();
    }

    JavaPairRDD<String, ProteomeEntry> loadProteomeRDD() {
        return proteomeRDDReader.load();
    }

    JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
        return proteomeStatisticsReader.getProteomeStatisticsRDD();
    }

    JavaPairRDD<String, TaxonomyEntry> getTaxonomyRDD() {
        return taxonomyRDDReader.load();
    }

    void saveToHPS(JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD) {
        String hpsPath =
                getCollectionOutputReleaseDirPath(
                        jobParameter.getApplicationConfig(),
                        jobParameter.getReleaseName(),
                        SolrCollection.proteome);
        SolrUtils.saveSolrInputDocumentRDD(proteomeDocumentJavaRDD, hpsPath);
    }
}

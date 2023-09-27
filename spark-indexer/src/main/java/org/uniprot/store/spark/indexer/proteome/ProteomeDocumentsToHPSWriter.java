package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeEntryToProteomeDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsToProteomeEntryMapper;
import org.uniprot.store.spark.indexer.proteome.reader.ProteomeStatisticsReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import scala.Tuple2;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

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
        JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD = loadProteomeRDD();
        proteomeIdProteomeEntryJavaPairRDD = joinStatistics(proteomeIdProteomeEntryJavaPairRDD);
        proteomeIdProteomeEntryJavaPairRDD = joinTaxonomy(proteomeIdProteomeEntryJavaPairRDD);
        JavaPairRDD<String, ProteomeDocument> idProteomeDocumentJavaPairRDD =
                proteomeIdProteomeEntryJavaPairRDD.mapValues(
                        getProteomeEntryToProteomeDocumentMapper());

        saveToHPS(idProteomeDocumentJavaPairRDD.values());
    }

    private JavaPairRDD<String, ProteomeEntry> joinTaxonomy(
            JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD) {
        // <organismId, proteomeId>
        JavaPairRDD<String, String> taxIdProteomeIdJavaRDD =
                proteomeIdProteomeEntryJavaPairRDD.mapToPair(
                        proteomeIdProteomeDocumentTuple2 ->
                                new Tuple2<>(
                                        String.valueOf(
                                                proteomeIdProteomeDocumentTuple2
                                                        ._2
                                                        .getTaxonomy()
                                                        .getTaxonId()),
                                        proteomeIdProteomeDocumentTuple2._1));
        // <taxId, taxEntry>
        JavaPairRDD<String, TaxonomyEntry> taxIdTaxEntryRDD = getTaxonomyRDD();
        // <proteomeId, taxonEntry>
        JavaPairRDD<String, TaxonomyEntry> proteomeIdTaxonomyEntryJavaPairRDD =
                taxIdProteomeIdJavaRDD.leftOuterJoin(taxIdTaxEntryRDD).values().mapToPair(
                        proteomeIdTaxEntryOptTuple2 ->
                                new Tuple2<>(proteomeIdTaxEntryOptTuple2._1,
                                                proteomeIdTaxEntryOptTuple2._2.orElse(null))
                );
        return proteomeIdProteomeEntryJavaPairRDD
                .join(proteomeIdTaxonomyEntryJavaPairRDD)
                .mapValues(v1 -> ProteomeEntryBuilder.from(v1._1).taxonomy(v1._2).build());
    }

    private JavaPairRDD<String, ProteomeEntry> joinStatistics(
            JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD) {
        return proteomeIdProteomeEntryJavaPairRDD // <proteomeId, proteomeEntry>
                .leftOuterJoin(getProteomeStatisticsRDD()) // <proteomeId, proteomeStatistics>
                .mapValues(getProteomeStatisticsToProteomeEntryMapper());
    }

    Function<Tuple2<ProteomeEntry, Optional<ProteomeStatistics>>, ProteomeEntry>
            getProteomeStatisticsToProteomeEntryMapper() {
        return new ProteomeStatisticsToProteomeEntryMapper();
    }

    Function<ProteomeEntry, ProteomeDocument> getProteomeEntryToProteomeDocumentMapper() {
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

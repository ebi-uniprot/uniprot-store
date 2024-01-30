package org.uniprot.store.spark.indexer.proteome;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.genecentric.GeneCentricCanonicalRDDReader;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeEntryToProteomeDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsToProteomeEntryMapper;
import org.uniprot.store.spark.indexer.proteome.mapper.TaxonomyToProteomeEntryMapper;
import org.uniprot.store.spark.indexer.proteome.reader.ProteomeStatisticsReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import scala.Tuple2;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

@Slf4j
public class ProteomeDocumentsToHPSWriter implements DocumentsToHPSWriter {
    private final JobParameter jobParameter;
    private final ProteomeRDDReader proteomeRDDReader;
    private final ProteomeStatisticsReader proteomeStatisticsReader;
    private final TaxonomyRDDReader taxonomyRDDReader;
    private final GeneCentricCanonicalRDDReader geneCentricCanonicalRDDReader;

    public ProteomeDocumentsToHPSWriter(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.proteomeRDDReader = new ProteomeRDDReader(jobParameter, true);
        this.proteomeStatisticsReader = new ProteomeStatisticsReader(jobParameter);
        this.taxonomyRDDReader = new TaxonomyRDDReader(jobParameter, true);
        this.geneCentricCanonicalRDDReader = new GeneCentricCanonicalRDDReader(jobParameter);
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD = loadProteomeRDD();
        proteomeIdProteomeEntryJavaPairRDD = joinStatistics(proteomeIdProteomeEntryJavaPairRDD);
        proteomeIdProteomeEntryJavaPairRDD = joinTaxonomy(proteomeIdProteomeEntryJavaPairRDD);
        proteomeIdProteomeEntryJavaPairRDD = joinGeneCounts(proteomeIdProteomeEntryJavaPairRDD);
        JavaPairRDD<String, ProteomeDocument> idProteomeDocumentJavaPairRDD =
                proteomeIdProteomeEntryJavaPairRDD.mapValues(
                        getProteomeEntryToProteomeDocumentMapper());

        saveToHPS(idProteomeDocumentJavaPairRDD.values());
    }

    private JavaPairRDD<String, ProteomeEntry> joinGeneCounts(
            JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD) {
        log.info("*********proteomes total"+proteomeIdProteomeEntryJavaPairRDD.count());
        JavaPairRDD<String, Integer> proteomeIdToProteomeGeneCount = getProteomeGeneCountRDD();
        log.info("*********genecount total"+proteomeIdToProteomeGeneCount.count());
        log.info("*********genecount distinct total"+proteomeIdToProteomeGeneCount.distinct().count());
        JavaPairRDD<String, ProteomeEntry> stringProteomeEntryJavaPairRDD = proteomeIdProteomeEntryJavaPairRDD
                .leftOuterJoin(proteomeIdToProteomeGeneCount)
                .values()
                .mapToPair(getProteomeGeneCountToProteomeEntryMapper());
        log.info("******************after adding gene total "+stringProteomeEntryJavaPairRDD.count());
        return stringProteomeEntryJavaPairRDD;
    }

    PairFunction<Tuple2<ProteomeEntry, Optional<Integer>>, String, ProteomeEntry>
    getProteomeGeneCountToProteomeEntryMapper() {
        return proteomeEntryProteomeGeneCountTuple2 ->
                new Tuple2<>(
                        proteomeEntryProteomeGeneCountTuple2._1.getId().getValue(),
                        ProteomeEntryBuilder.from(proteomeEntryProteomeGeneCountTuple2._1)
                                .geneCount(proteomeEntryProteomeGeneCountTuple2._2.orElse(0))
                                .build());
    }

    JavaPairRDD<String, Integer> getProteomeGeneCountRDD() {
        return geneCentricCanonicalRDDReader.loadProteomeGeneCounts();
    }

    private JavaPairRDD<String, ProteomeEntry> joinTaxonomy(
            JavaPairRDD<String, ProteomeEntry> proteomeIdProteomeEntryJavaPairRDD) {
        // <organismId, proteomeId>
        JavaPairRDD<String, String> taxIdProteomeIdJavaRDD =
                proteomeIdProteomeEntryJavaPairRDD.mapToPair(
                        proteomeIdProteomeEntryTuple2 ->
                                new Tuple2<>(
                                        String.valueOf(
                                                proteomeIdProteomeEntryTuple2
                                                        ._2
                                                        .getTaxonomy()
                                                        .getTaxonId()),
                                        proteomeIdProteomeEntryTuple2._1));

        // <taxId, taxEntry>
        JavaPairRDD<String, TaxonomyEntry> taxIdTaxEntryRDD = getTaxonomyRDD();

        // <proteomeId, taxonEntry>
        JavaPairRDD<String, Optional<TaxonomyEntry>> proteomeIdTaxonomyEntryJavaPairRDD =
                taxIdProteomeIdJavaRDD
                        .leftOuterJoin(taxIdTaxEntryRDD)
                        .values()
                        .mapToPair(
                                proteomeIdTaxEntryOptTuple2 ->
                                        new Tuple2<>(
                                                proteomeIdTaxEntryOptTuple2._1,
                                                proteomeIdTaxEntryOptTuple2._2));

        return proteomeIdProteomeEntryJavaPairRDD
                .join(proteomeIdTaxonomyEntryJavaPairRDD)
                .mapValues(getTaxonomyToProteomeEntryMapper());
    }

    Function<Tuple2<ProteomeEntry, Optional<TaxonomyEntry>>, ProteomeEntry>
    getTaxonomyToProteomeEntryMapper() {
        return new TaxonomyToProteomeEntryMapper();
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

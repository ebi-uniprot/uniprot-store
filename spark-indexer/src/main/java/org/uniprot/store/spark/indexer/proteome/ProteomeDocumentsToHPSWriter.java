package org.uniprot.store.spark.indexer.proteome;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
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
        JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD =
                loadProteomeRDD()
                        .mapValues(getEntryToProteomeDocumentMapper())
                        .leftOuterJoin(getProteomeStatisticsRDD())
                        .mapValues(getStatisticsToProteomeDocumentMapper())
                        .mapValues(getTaxonomyToProteomeDocumentMapper())
                        .values();

        saveToHPS(proteomeDocumentJavaRDD);
    }

    Function<ProteomeDocument, ProteomeDocument> getTaxonomyToProteomeDocumentMapper() {
        return new TaxonomyToProteomeDocumentMapper(getTaxonomyEntryMap());
    }

    Function<Tuple2<ProteomeDocument, Optional<ProteomeStatistics>>, ProteomeDocument>
            getStatisticsToProteomeDocumentMapper() {
        return new StatisticsToProteomeDocumentMapper();
    }

    Function<ProteomeEntry, ProteomeDocument> getEntryToProteomeDocumentMapper() {
        return new ProteomeEntryToProteomeDocumentMapper();
    }

    JavaPairRDD<String, ProteomeEntry> loadProteomeRDD() {
        return proteomeRDDReader.load().persist(StorageLevel.DISK_ONLY());
    }

    JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
        return proteomeStatisticsReader.getProteomeStatisticsRDD();
    }

    Map<String, TaxonomyEntry> getTaxonomyEntryMap() {
        return taxonomyRDDReader.load().collectAsMap();
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

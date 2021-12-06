package org.uniprot.store.spark.indexer.taxonomy;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.Objects;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.proteome.ProteomeRDDReader;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeTaxonomyStatisticsMapper;
import org.uniprot.store.spark.indexer.taxonomy.mapper.*;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;
import org.uniprot.store.spark.indexer.taxonomy.reader.*;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.OrganismJoinMapper;

/**
 * This class is responsible to load all the data for TaxonomyDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2021-09-22
 */
@Slf4j
public class TaxonomyDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter parameter;
    private final ResourceBundle config;
    private final String releaseName;

    public TaxonomyDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        TaxonomyRDDReader taxonomyRDDReader = getTaxonomyRDDReader();
        JavaPairRDD<String, TaxonomyEntry> taxonomyRDD =
                taxonomyRDDReader.load().persist(StorageLevel.DISK_ONLY());

        TaxonomyLinksRDDReader linksRddReader = new TaxonomyLinksRDDReader(parameter);
        JavaPairRDD<String, Iterable<TaxonomyEntry>> linksRDD = linksRddReader.load().groupByKey();

        TaxonomyOtherNamesRDDReader otherNamesRddReader =
                new TaxonomyOtherNamesRDDReader(parameter);
        JavaPairRDD<String, Iterable<TaxonomyEntry>> otherNamesRDD =
                otherNamesRddReader.load().groupByKey();

        TaxonomyStrainsRDDReader strainsRddReader = new TaxonomyStrainsRDDReader(parameter);
        JavaPairRDD<String, Iterable<Strain>> strainsRDD = strainsRddReader.load().groupByKey();

        JavaPairRDD<String, TaxonomyStatisticsWrapper> proteinStatisticsRDD =
                getTaxonomyProteinStatisticsRDD(taxonomyRDD);

        JavaPairRDD<String, TaxonomyStatisticsWrapper> proteomeStatisticsRDD =
                getTaxonomyProteomeStatisticsRDD(taxonomyRDD);

        JavaPairRDD<String, TaxonomyStatisticsWrapper> statisticsWrapperRDD =
                proteinStatisticsRDD
                        .fullOuterJoin(proteomeStatisticsRDD)
                        .mapValues(new TaxonomyStatisticsWrapperJoinMapper());

        JavaRDD<TaxonomyDocument> taxonomyDocumentRDD =
                taxonomyRDD // <taxId, TaxonomyEntry>
                        .leftOuterJoin(linksRDD)
                        .mapValues(new TaxonomyLinksJoinMapper())
                        .leftOuterJoin(otherNamesRDD)
                        .mapValues(new TaxonomyOtherNamesJoinMapper())
                        .leftOuterJoin(strainsRDD)
                        .mapValues(new TaxonomyStrainsJoinMapper())
                        .leftOuterJoin(getTaxonomyHosts(taxonomyRDD))
                        .mapValues(new TaxonomyHostsJoinMapper())
                        .leftOuterJoin(statisticsWrapperRDD)
                        .mapValues(new TaxonomyEntryToDocumentMapper())
                        .values()
                        .union(getInactiveDocumentsRDD())
                        .filter(Objects::nonNull);

        saveToHDFS(taxonomyDocumentRDD);

        log.info("Completed Taxonomy prepare Solr index");
    }

    TaxonomyRDDReader getTaxonomyRDDReader() {
        return new TaxonomyRDDReader(parameter, true);
    }

    void saveToHDFS(JavaRDD<TaxonomyDocument> taxonomyDocumentRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.taxonomy);
        SolrUtils.saveSolrInputDocumentRDD(taxonomyDocumentRDD, hdfsPath);
    }

    private JavaPairRDD<String, Iterable<Taxonomy>> getTaxonomyHosts(
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        TaxonomyHostsRDDReader hostsRDDReader = new TaxonomyHostsRDDReader(parameter);
        return hostsRDDReader
                .load() // <hostId,TaxId>
                .groupByKey() // <hostId, List<taxId>>
                .leftOuterJoin(taxonomyRDD) // <hostId, List<taxId>, Optional<TaxonomyHostEntry>>
                .flatMapToPair(
                        new TaxonomyHostsAndEntryJoinMapper()) // <taxId, List<TaxonomyHosts>>
                .groupByKey();
    }

    private JavaRDD<TaxonomyDocument> getInactiveDocumentsRDD() {
        TaxonomyDeletedRDDReader deletedRDDReader = new TaxonomyDeletedRDDReader(parameter);
        TaxonomyMergedRDDReader mergedRDDReader = new TaxonomyMergedRDDReader(parameter);

        return deletedRDDReader.load().union(mergedRDDReader.load());
    }

    /**
     * This method load TaxonomyStatistics with protein statistics. The statistics value must
     * consider lineage as well
     *
     * @param taxonomyRDD taxonomy entries
     * @return JavaPairRDD<taxId, TaxonomyStatistics>
     */
    private JavaPairRDD<String, TaxonomyStatisticsWrapper> getTaxonomyProteinStatisticsRDD(
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        TaxonomyStatisticsAggregationMapper statisticsAggregationMapper =
                new TaxonomyStatisticsAggregationMapper();

        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);

        JavaPairRDD<String, TaxonomyStatisticsWrapper> organismStatisticsRDD =
                uniProtKBReader
                        .loadFlatFileToRDD()
                        .mapToPair(new OrganismJoinMapper()) // <taxId, TaxonomyStatistics>
                        .aggregateByKey(
                                null, statisticsAggregationMapper, statisticsAggregationMapper);

        return taxonomyRDD // <taxId, TaxonomyEntry>
                .leftOuterJoin(organismStatisticsRDD)
                .values() // <TaxonomyEntry, Optional<TaxonomyStatistics>>
                .flatMapToPair(new LineageStatisticsMapper()) // <taxId, TaxonomyStatistics>
                .aggregateByKey(null, statisticsAggregationMapper, statisticsAggregationMapper);
    }

    /**
     * This method load TaxonomyStatistics with proteome statistics.
     *
     * @param taxonomyRDD taxonomy entries
     * @return JavaPairRDD<taxId, TaxonomyStatistics>
     */
    private JavaPairRDD<String, TaxonomyStatisticsWrapper> getTaxonomyProteomeStatisticsRDD(
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        TaxonomyStatisticsAggregationMapper statisticsAggregationMapper =
                new TaxonomyStatisticsAggregationMapper();
        ProteomeRDDReader proteomeRDDReader = new ProteomeRDDReader(this.parameter, false);

        return proteomeRDDReader
                .load()
                .values() // ProteomeEntry
                .mapToPair(new ProteomeTaxonomyStatisticsMapper()) // <taxId, TaxonomyStatistics>
                .aggregateByKey(null, statisticsAggregationMapper, statisticsAggregationMapper);
    }
}

package org.uniprot.store.spark.indexer.subcellularlocation;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.subcell.SubcellularLocationRDDReader;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.StatisticsAggregationMapper;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationEntryToDocument;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationJoinMapper;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationFlatAncestor;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellullarLocationStatisticsMerger;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

/**
 * @author sahmad
 * @created 31/01/2022
 */
@Slf4j
public class SubcellularLocationDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter jobParameter;
    private final ResourceBundle appConfig;
    private final String releaseName;
    private final UniProtKBRDDTupleReader uniProtKBReader;

    public SubcellularLocationDocumentsToHDFSWriter(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.appConfig = jobParameter.getApplicationConfig();
        this.releaseName = jobParameter.getReleaseName();
        this.uniProtKBReader = new UniProtKBRDDTupleReader(this.jobParameter, true);
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        StatisticsAggregationMapper aggregationMapper = new StatisticsAggregationMapper();
        // read uniprotkb and get Tuple2 <SL-XXXX, Statistics>
        JavaPairRDD<String, Statistics> slIdStatsRDD = this.uniProtKBReader.load()
                .flatMapToPair(new SubcellularLocationJoinMapper())
                .aggregateByKey(null, aggregationMapper, aggregationMapper);

        SubcellularLocationRDDReader subcellReader = new SubcellularLocationRDDReader(this.jobParameter);
        SubcellullarLocationStatisticsMerger subcellMerger = new SubcellullarLocationStatisticsMerger();
        // read subcellular input file in RDD<SL-xxxx, SLEntry>
        JavaPairRDD<String, SubcellularLocationEntry> subcellRDD = subcellReader.load();// small data set
        // get RDD with total count for each id but children's(ancestors) counts are not properly updated
        // RDD should have 500+ records (same number as in input file subcell.txt)
        // RDD<SL-xxxx, Entry>
        JavaPairRDD<String, SubcellularLocationEntry> slIdEntryWithStatsRDD = subcellRDD
                .leftOuterJoin(slIdStatsRDD)
                .values()
                .flatMapToPair(new SubcellularLocationFlatAncestor())
                .aggregateByKey(null, subcellMerger, subcellMerger);
        // convert to solr document to save in hdfs
        JavaRDD<SubcellularLocationDocument> subcellDocumentRDD = slIdEntryWithStatsRDD.mapValues(new SubcellularLocationEntryToDocument()).values();
        saveToHDFS(subcellDocumentRDD);
    }

    void saveToHDFS(JavaRDD<SubcellularLocationDocument> subcellDocumentRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(appConfig, releaseName, SolrCollection.subcellularlocation);
        SolrUtils.saveSolrInputDocumentRDD(subcellDocumentRDD, hdfsPath);
        log.info("Completed SubcellularLocation prepare Solr index");
    }
}

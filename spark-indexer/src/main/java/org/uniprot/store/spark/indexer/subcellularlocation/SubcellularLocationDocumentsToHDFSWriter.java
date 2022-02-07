package org.uniprot.store.spark.indexer.subcellularlocation;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.HashSet;
import java.util.Map;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

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
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.CombineFunction;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.MappedProteinAccession;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.MergeFunction;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.StatisticsAggregationMapper;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationEntryToDocument;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationFlatAncestor;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationJoinMapper;
import org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellullarLocationStatisticsMerger;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

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
        // read uniprotkb and get Tuple2 <SL-XXXX, MappedProteinAccession>
        JavaPairRDD<String, Iterable<MappedProteinAccession>> subcellIdProteinsRDD = this.uniProtKBReader
                .load()
                .flatMapToPair(new SubcellularLocationJoinMapper())
                .groupByKey();// use aggregateByKey TODO

        SubcellularLocationRDDReader subcellReader =
                new SubcellularLocationRDDReader(this.jobParameter);
        // read subcellular input file in RDD<SL-xxxx, SLEntry>
        JavaPairRDD<String, SubcellularLocationEntry> subcellRDD =
                subcellReader.load(); // small data set

        // RDD<SL-xxxx, Statistics>
        JavaPairRDD<String, Statistics> subcellIdStatsRDD = subcellRDD
                .leftOuterJoin(subcellIdProteinsRDD)
                .values()
                .flatMapToPair(new SubcellularLocationFlatAncestor())
                .aggregateByKey(new HashSet<>(), new CombineFunction(), new MergeFunction())
                .mapValues(new StatisticsAggregationMapper());

        Map<String, Statistics> subcellIdStatsMap = subcellIdStatsRDD.collectAsMap();
        // convert to solr document to save in hdfs
        JavaRDD<SubcellularLocationDocument> subcellDocumentRDD =
                subcellReader.load().mapValues(new SubcellularLocationEntryToDocument(subcellIdStatsMap)).values();
        saveToHDFS(subcellDocumentRDD);
    }

    void saveToHDFS(JavaRDD<SubcellularLocationDocument> subcellDocumentRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(
                        appConfig, releaseName, SolrCollection.subcellularlocation);
        SolrUtils.saveSolrInputDocumentRDD(subcellDocumentRDD, hdfsPath);
        log.info("Completed SubcellularLocation prepare Solr index");
    }
}

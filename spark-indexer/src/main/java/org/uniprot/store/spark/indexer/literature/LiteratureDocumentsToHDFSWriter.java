package org.uniprot.store.spark.indexer.literature;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.literature.mapper.LiteratureEntryStatisticsJoin.*;
import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.literature.mapper.*;
import org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
@Slf4j
public class LiteratureDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter parameter;

    public LiteratureDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        LiteratureRDDTupleReader literatureReader = new LiteratureRDDTupleReader(parameter);

        // load literature with abstract JavaPairRDD<citationId, Literature>
        JavaPairRDD<String, Literature> literature = literatureReader.load();

        MappedReferenceRDDReader mappedRefReader =
                new MappedReferenceRDDReader(parameter, KeyType.CITATION_ID);
        // load computational Stats JavaPairRDD<citationId, count>
        JavaPairRDD<String, Long> computationalStatsRDD =
                mappedRefReader
                        .loadComputationalMappedReference()
                        .mapValues(ref -> 1L)
                        .reduceByKey(Long::sum);

        // load community  Stats JavaPairRDD<citationId, count>
        JavaPairRDD<String, Long> communityStatsRDD =
                mappedRefReader
                        .loadCommunityMappedReference()
                        .mapValues(ref -> 1L)
                        .reduceByKey(Long::sum);

        JavaRDD<LiteratureDocument> literatureDocsRDD =
                loadUniProtKBLiteratureEntryRDD()
                        .fullOuterJoin(literature)
                        .mapValues(new LiteratureUniProtKBJoin())
                        .leftOuterJoin(communityStatsRDD)
                        .mapValues(new LiteratureEntryStatisticsJoin(StatisticsType.COMMUNITY))
                        .leftOuterJoin(computationalStatsRDD)
                        .mapValues(
                                new LiteratureEntryStatisticsJoin(StatisticsType.COMPUTATIONALLY))
                        .mapValues(new LiteratureEntryToDocumentMapper())
                        .values();

        saveToHDFS(literatureDocsRDD);

        log.info("Completed writing literature documents to HDFS");
    }

    private JavaPairRDD<String, LiteratureEntry> loadUniProtKBLiteratureEntryRDD() {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        JavaRDD<String> uniProtKBEntryStringsRDD = uniProtKBReader.loadFlatFileToRDD();

        return uniProtKBEntryStringsRDD
                .flatMapToPair(new LiteratureEntryUniProtKBMapper())
                .aggregateByKey(
                        null,
                        new LiteratureEntryAggregationMapper(),
                        new LiteratureEntryAggregationMapper());
    }

    void saveToHDFS(JavaRDD<LiteratureDocument> literatureDocsRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(
                        parameter.getApplicationConfig(),
                        parameter.getReleaseName(),
                        SolrCollection.literature);
        SolrUtils.saveSolrInputDocumentRDD(literatureDocsRDD, hdfsPath);
    }
}

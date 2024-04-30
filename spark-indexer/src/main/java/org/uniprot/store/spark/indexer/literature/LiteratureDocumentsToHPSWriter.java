package org.uniprot.store.spark.indexer.literature;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.literature.mapper.LiteratureEntryStatisticsJoin.*;
import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.literature.mapper.*;
import org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
@Slf4j
public class LiteratureDocumentsToHPSWriter implements DocumentsToHPSWriter {

    private final JobParameter parameter;

    public LiteratureDocumentsToHPSWriter(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        LiteratureRDDTupleReader literatureReader = new LiteratureRDDTupleReader(parameter);

        // load literature with abstract JavaPairRDD<citationId, Literature>
        JavaPairRDD<String, Literature> literatureRDD = literatureReader.load();

        MappedReferenceRDDReader mappedRefReader =
                new MappedReferenceRDDReader(parameter, KeyType.ACCESSION_AND_CITATION_ID);
        // load computational Stats JavaPairRDD<citationId, count>
        JavaPairRDD<String, Long> computationalStatsRDD =
                mappedRefReader
                        .loadComputationalMappedReference()
                        .mapValues(ref -> 1L)
                        .reduceByKey((value1, value2) -> value1) // remove duplicated keys
                        .mapToPair(new LiteratureMappedRefStatsMapper())
                        .reduceByKey(Long::sum);

        // load community  Stats JavaPairRDD<citationId, count>
        JavaPairRDD<String, Long> communityStatsRDD =
                mappedRefReader
                        .loadCommunityMappedReference()
                        .mapValues(ref -> 1L)
                        .reduceByKey((value1, value2) -> value1) // remove duplicated keys
                        .mapToPair(new LiteratureMappedRefStatsMapper())
                        .reduceByKey(Long::sum);

        JavaRDD<LiteratureDocument> literatureDocsRDD =
                loadUniProtKBLiteratureEntryRDD()
                        .fullOuterJoin(literatureRDD)
                        .mapValues(new LiteratureUniProtKBJoin())
                        .leftOuterJoin(communityStatsRDD)
                        .mapValues(new LiteratureEntryStatisticsJoin(StatisticsType.COMMUNITY))
                        .leftOuterJoin(computationalStatsRDD)
                        .mapValues(
                                new LiteratureEntryStatisticsJoin(StatisticsType.COMPUTATIONALLY))
                        .mapValues(new LiteratureEntryToDocumentMapper())
                        .values();

        saveToHPS(literatureDocsRDD);

        log.info("Completed writing literature documents to HPS");
    }

    private JavaPairRDD<String, LiteratureEntry> loadUniProtKBLiteratureEntryRDD() {
        LiteratureUniProtKBRDDReader literatureUniProtKBRDDReader =
                new LiteratureUniProtKBRDDReader(parameter);
        return literatureUniProtKBRDDReader.load();
    }

    void saveToHPS(JavaRDD<LiteratureDocument> literatureDocsRDD) {
        String hpsPath =
                getCollectionOutputReleaseDirPath(
                        parameter.getApplicationConfig(),
                        parameter.getReleaseName(),
                        SolrCollection.literature);
        SolrUtils.saveSolrInputDocumentRDD(literatureDocsRDD, hpsPath);
    }
}

package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsAggregationMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import scala.Tuple2;

import java.util.List;
import java.util.ResourceBundle;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

public class ProteomeDocumentsToHDFSWriter implements DocumentsToHDFSWriter {
    private final JobParameter parameter;
    private final ResourceBundle config;
    private final String releaseName;

    public ProteomeDocumentsToHDFSWriter(JobParameter parameter, ResourceBundle config, String releaseName) {
        this.parameter = parameter;
        this.config = config;
        this.releaseName = releaseName;
    }


    @Override
    public void writeIndexDocumentsToHDFS() {
        ProteomeRDDReader proteomeRDDReader = getProteomeRDDReader();
        JavaPairRDD<String, ProteomeEntry> proteomeEntryJavaPairRDD = proteomeRDDReader.load().persist(StorageLevel.DISK_ONLY());
        JavaRDD<Tuple2<ProteomeEntry, Optional<ProteomeStatistics>>> proteomeStatisticsRDD = getProteomeStatisticsRDD(proteomeEntryJavaPairRDD);

        JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD = proteomeEntryJavaPairRDD
                .leftOuterJoin(proteomeStatisticsRDD)
                .mapValues();

        saveToHDFS(proteomeDocumentJavaRDD);
    }

    private JavaRDD<Tuple2<ProteomeEntry, Optional<ProteomeStatistics>>> getProteomeStatisticsRDD(JavaPairRDD<String, ProteomeEntry> proteomeRDD) {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        ProteomeStatisticsAggregationMapper agg = new ProteomeStatisticsAggregationMapper();

        JavaPairRDD<String, ProteomeStatistics> proteomeStatisticsRDD = uniProtKBReader.loadFlatFileToRDD()
                .map(new ProteomeJoinMapper())
                .flatMap(List::iterator)
                .mapToPair(stringProteomeStatisticsTuple2 -> stringProteomeStatisticsTuple2)
                .aggregateByKey(null, agg, agg);

        return proteomeRDD
                .leftOuterJoin(proteomeStatisticsRDD)
                .values();
    }
    void saveToHDFS(JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.proteome);
        SolrUtils.saveSolrInputDocumentRDD(proteomeDocumentJavaRDD, hdfsPath);
    }

    ProteomeRDDReader getProteomeRDDReader() {
        return new ProteomeRDDReader(parameter, true);
    }
}

package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.proteome.mapper.ProteomeStatisticsAggregationMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import java.util.List;
import java.util.ResourceBundle;

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

    }

    private JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD(JavaPairRDD<String, ProteomeEntry> taxonomyRDD) {
        //statistics aggregation mapper init

        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        ProteomeStatisticsAggregationMapper agg = new ProteomeStatisticsAggregationMapper();

        JavaPairRDD<String, ProteomeStatistics> proteomeStats = uniProtKBReader.loadFlatFileToRDD()
                .map(new ProteomeJoinMapper())
                .flatMap(List::iterator)
                .mapToPair(stringProteomeStatisticsTuple2 -> stringProteomeStatisticsTuple2)
                .aggregateByKey(null, agg, agg);

        return proteomeStats
    }
}

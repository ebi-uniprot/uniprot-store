package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.proteome.mapper.model.ProteomeStatisticsWrapper;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

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

    private JavaPairRDD<String, ProteomeStatisticsWrapper> getProteomeStatisticsRDD(JavaPairRDD<String, ProteomeEntry> taxonomyRDD) {
        //statistics aggregation mapper init

        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);

        JavaPairRDD<String, ProteomeStatisticsWrapper> proteomeStatisticsRDD =
        uniProtKBReader.loadFlatFileToRDD()
                .mapToPair(new ProteomeJoinMapper())
                .aggregateByKey(null,)
    }
}

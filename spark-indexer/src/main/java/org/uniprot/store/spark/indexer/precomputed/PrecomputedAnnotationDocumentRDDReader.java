package org.uniprot.store.spark.indexer.precomputed;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;
import static org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader.SPLITTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.precomputed.mapper.PrecomputedAnnotationEntryToDocument;

import com.typesafe.config.Config;

public class PrecomputedAnnotationDocumentRDDReader
        implements PairRDDReader<String, PrecomputedAnnotationDocument> {

    private final JobParameter jobParameter;

    public PrecomputedAnnotationDocumentRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, PrecomputedAnnotationDocument> load() {
        Config config = this.jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String fullFilePath = releaseInputDir + config.getString("precomputed.annotation.file");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return jsc.textFile(fullFilePath)
                .map(entry -> entry + SPLITTER)
                .mapToPair(new PrecomputedAnnotationEntryToDocument());
    }

    public JavaRDD<PrecomputedAnnotationDocument> loadDocuments() {
        return load().values();
    }
}

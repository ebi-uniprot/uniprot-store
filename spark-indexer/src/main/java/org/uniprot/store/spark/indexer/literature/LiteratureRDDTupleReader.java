package org.uniprot.store.spark.indexer.literature;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.citation.Literature;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureFileMapper;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
public class LiteratureRDDTupleReader implements PairRDDReader<String, Literature> {

    private static final String SPLITTER = "\n//\n";

    private final JobParameter jobParameter;

    public LiteratureRDDTupleReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * load Literature to a JavaPairRDD
     *
     * @return JavaPairRDD{key=citationId, value=Literature}
     */
    @Override
    public JavaPairRDD<String, Literature> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String literaturePath = releaseInputDir + config.getString("literature.file.path");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return jsc.textFile(literaturePath).mapToPair(new LiteratureFileMapper());
    }
}

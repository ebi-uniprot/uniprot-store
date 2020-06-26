package org.uniprot.store.spark.indexer.subcell;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseMainThreadDirPath;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.cv.subcell.SubcellularLocationFileReader;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 2020-01-16
 */
public class SubcellularLocationRDDReader
        implements PairRDDReader<String, SubcellularLocationEntry> {

    private final JobParameter jobParameter;

    public SubcellularLocationRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD{key=subcellId, value={@link SubcellularLocationEntry}} */
    @Override
    public JavaPairRDD<String, SubcellularLocationEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        String releaseInputDir =
                getInputReleaseMainThreadDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("subcell.file.path");
        SubcellularLocationFileReader fileReader = new SubcellularLocationFileReader();
        List<String> lines = SparkUtils.readLines(filePath, jsc.hadoopConfiguration());
        List<SubcellularLocationEntry> entries = fileReader.parseLines(lines);

        return jsc.parallelize(entries).mapToPair(new SubcellularLocationMapper());
    }
}

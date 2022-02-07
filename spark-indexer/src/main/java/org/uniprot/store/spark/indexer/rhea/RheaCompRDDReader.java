package org.uniprot.store.spark.indexer.rhea;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

public class RheaCompRDDReader implements PairRDDReader<String, RheaComp> {

    private final JobParameter jobParameter;

    public RheaCompRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, RheaComp> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("rhea.comp.file.path");

        SparkSession session =
                SparkSession.builder()
                        .sparkContext(jobParameter.getSparkContext().sc())
                        .getOrCreate();

        return session.read().textFile(filePath).toJavaRDD().mapToPair(new RheaCompFileMapper());
    }
}

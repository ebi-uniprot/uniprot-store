package org.uniprot.store.spark.indexer.go.evidence;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;

import com.typesafe.config.Config;

/**
 * This class load extended GO Evidences into an JavaPairRDD of {key=uniprot accession,
 * value=Iterable of GOEvidence}
 *
 * @author lgonzales
 * @since 2019-10-13
 */
@Slf4j
public class GOEvidencesRDDReader implements PairRDDReader<String, Iterable<GOEvidence>> {

    private final JobParameter jobParameter;

    public GOEvidencesRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /** @return JavaPairRDD of {key=uniprot accession, value=Iterable of GoEvidence} */
    public JavaPairRDD<String, Iterable<GOEvidence>> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String goEvidencePath = releaseInputDir + config.getString("go.evidence.file.path");
        return spark.read()
                .textFile(goEvidencePath)
                .toJavaRDD()
                .mapToPair(new GOEvidencesFileMapper())
                .groupByKey();
    }
}

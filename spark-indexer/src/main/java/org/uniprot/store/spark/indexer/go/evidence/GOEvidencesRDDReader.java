package org.uniprot.store.spark.indexer.go.evidence;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;

/**
 * This class load extended GO Evidences into an JavaPairRDD of {key=uniprot accession,
 * value=Iterable of GOEvidence}
 *
 * @author lgonzales
 * @since 2019-10-13
 */
@Slf4j
public class GOEvidencesRDDReader {

    private GOEvidencesRDDReader() {}

    /** @return JavaPairRDD of {key=uniprot accession, value=Iterable of GoEvidence} */
    public static JavaPairRDD<String, Iterable<GOEvidence>> load(JobParameter jobParameter) {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String goEvidencePath = releaseInputDir + config.getString("go.evidence.file.path");
        return (JavaPairRDD<String, Iterable<GOEvidence>>)
                spark.read()
                        .textFile(goEvidencePath)
                        .toJavaRDD()
                        .mapToPair(new GOEvidencesFileMapper())
                        .groupByKey();
    }
}

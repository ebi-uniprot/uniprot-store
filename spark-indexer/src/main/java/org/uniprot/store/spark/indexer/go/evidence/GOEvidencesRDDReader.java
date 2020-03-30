package org.uniprot.store.spark.indexer.go.evidence;

import static org.uniprot.store.spark.indexer.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 * This class load extended GO Evidences into an JavaPairRDD of {key=uniprot accession,
 * value=Iterable of GOEvidence}
 *
 * @author lgonzales
 * @since 2019-10-13
 */
@Slf4j
public class GOEvidencesRDDReader {

    /** @return JavaPairRDD of {key=uniprot accession, value=Iterable of GoEvidence} */
    public static JavaPairRDD<String, Iterable<GOEvidence>> load(
            SparkConf sparkConf, ResourceBundle applicationConfig, String releaseName) {
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(applicationConfig, releaseName);
        String goEvidencePath =
                releaseInputDir + applicationConfig.getString("go.evidence.file.path");
        return (JavaPairRDD<String, Iterable<GOEvidence>>)
                spark.read()
                        .textFile(goEvidencePath)
                        .toJavaRDD()
                        .mapToPair(new GOEvidencesFileMapper())
                        .groupByKey();
    }
}

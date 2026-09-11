package org.uniprot.store.spark.indexer.publication;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.publication.mapper.CommunityMappedReferenceMapper;
import org.uniprot.store.spark.indexer.publication.mapper.ComputationallyMappedReferenceMapper;
import org.uniprot.store.spark.indexer.publication.mapper.MappedReferencePairMapper;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
public class MappedReferenceRDDReader {

    public enum KeyType {
        CITATION_ID,
        ACCESSION_AND_CITATION_ID
    }

    private final JobParameter parameter;
    private final KeyType keyType;

    public MappedReferenceRDDReader(JobParameter parameter, KeyType keyType) {
        this.parameter = parameter;
        this.keyType = keyType;
    }

    public JavaPairRDD<String, MappedReference> loadComputationalMappedReference() {
        return loadMappedReferenceRDD(
                "computational.mapped.references.file.path",
                new ComputationallyMappedReferenceMapper());
    }

    public JavaPairRDD<String, MappedReference> loadCommunityMappedReference() {
        return loadMappedReferenceRDD(
                "community.mapped.references.file.path", new CommunityMappedReferenceMapper());
    }

    private JavaPairRDD<String, MappedReference> loadMappedReferenceRDD(
            String srcFilePathProperty, Function<String, MappedReference> converter) {
        Config config = this.parameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, this.parameter.getReleaseName());
        String filePath = releaseInputDir + config.getString(srcFilePathProperty);

        JavaSparkContext jsc = this.parameter.getSparkContext();
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        JavaRDD<String> rawMappedRefStrRdd = spark.read().textFile(filePath).toJavaRDD();

        return rawMappedRefStrRdd
                .map(line -> convertMappedReference(filePath, converter, line))
                .mapToPair(new MappedReferencePairMapper(keyType));
    }

    private static MappedReference convertMappedReference(
            String filePath, Function<String, MappedReference> converter, String line) {
        try {
            return converter.call(line);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to convert mapped reference from "
                            + filePath
                            + ". line="
                            + abbreviate(line),
                    e);
        }
    }

    private static String abbreviate(String line) {
        if (line == null) {
            return null;
        }
        int maxLength = 500;
        if (line.length() <= maxLength) {
            return line;
        }
        return line.substring(0, maxLength) + "...";
    }
}

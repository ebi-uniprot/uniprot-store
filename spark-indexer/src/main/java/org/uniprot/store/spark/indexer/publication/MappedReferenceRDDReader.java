package org.uniprot.store.spark.indexer.publication;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

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
        ResourceBundle config = this.parameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, this.parameter.getReleaseName());
        String filePath = releaseInputDir + config.getString(srcFilePathProperty);

        JavaSparkContext jsc = this.parameter.getSparkContext();
        SparkSession spark = SparkSession.builder().config(jsc.getConf()).getOrCreate();
        JavaRDD<String> rawMappedRefStrRdd = spark.read().textFile(filePath).toJavaRDD();

        return rawMappedRefStrRdd.map(converter).mapToPair(new MappedReferencePairMapper(keyType));
    }
}

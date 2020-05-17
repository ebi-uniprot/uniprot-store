package org.uniprot.store.spark.indexer.uniref;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.disease.DiseaseRDDReader;
import scala.Tuple2;

import java.util.ResourceBundle;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class UniRefRDDTupleReaderTest {

    @Test
    void testLoadUniRef50WithPartition() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaRDD<UniRefEntry> uniref50Rdd = UniRefRDDTupleReader.load(UniRefType.UniRef50, parameter, true);
            assertNotNull(uniref50Rdd);
            long count = uniref50Rdd.count();
            assertEquals(1L, count);
            UniRefEntry entry = uniref50Rdd.first();

            assertNotNull(entry);
            assertEquals(UniRefType.UniRef50, entry.getEntryType());
            assertEquals("UniRef50_Q9EPI6", entry.getId().getValue());
        }
    }

    @Test
    void testLoadUniRef100WithWithoutPartition() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaRDD<UniRefEntry> uniref50Rdd = UniRefRDDTupleReader.load(UniRefType.UniRef100, parameter, false);
            assertNotNull(uniref50Rdd);
            long count = uniref50Rdd.count();
            assertEquals(1L, count);
            UniRefEntry entry = uniref50Rdd.first();

            assertNotNull(entry);
            assertEquals(UniRefType.UniRef100, entry.getEntryType());
            assertEquals("UniRef100_Q9EPI6", entry.getId().getValue());
        }
    }
}
package org.uniprot.store.spark.indexer.proteome;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.RelatedProteome;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author sahmad
 * @since 21/08/2020
 */
class ProteomeRDDReaderTest {
    @Test
    void testLoadProteomeWithoutPartition() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            ProteomeRDDReader reader = new ProteomeRDDReader(parameter, false);
            JavaPairRDD<String, ProteomeEntry> javaPairRDD = reader.load();
            assertNotNull(javaPairRDD);
            long count = javaPairRDD.count();
            assertEquals(7L, count);
            Tuple2<String, ProteomeEntry> firstTuple = javaPairRDD.first();
            assertNotNull(firstTuple);
            assertEquals("UP000000718", firstTuple._1);
            assertNotNull(firstTuple._2.getTaxonomy());
            assertEquals(289376L, firstTuple._2.getTaxonomy().getTaxonId());
            ProteomeEntry proteomeEntry = firstTuple._2;
            assertNotNull(proteomeEntry);
            assertEquals(14L, proteomeEntry.getPanproteomeTaxon().getTaxonId());
            assertEquals(3, proteomeEntry.getRelatedProteomes().size());
            RelatedProteome relatedProteome = proteomeEntry.getRelatedProteomes().get(0);
            assertNotNull(relatedProteome);
            assertNotNull(relatedProteome.getTaxId());
            assertNotNull(relatedProteome.getSimilarity());
            assertNotNull(relatedProteome.getId());
            javaPairRDD.foreach(tuple -> assertEquals(tuple._2.getId().getValue(), tuple._1));
        }
    }

    @Test
    void testLoadProteomeWithPartition() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            ProteomeRDDReader reader = new ProteomeRDDReader(parameter, true);
            JavaPairRDD<String, ProteomeEntry> javaPairRDD = reader.load();
            assertNotNull(javaPairRDD);
            long count = javaPairRDD.count();
            assertEquals(7L, count);
            Tuple2<String, ProteomeEntry> firstTuple = javaPairRDD.first();
            assertNotNull(firstTuple);
            assertEquals("UP000000718", firstTuple._1);
            assertNotNull(firstTuple._2.getTaxonomy());
            assertEquals(289376L, firstTuple._2.getTaxonomy().getTaxonId());
            javaPairRDD.foreach(tuple -> assertEquals(tuple._2.getId().getValue(), tuple._1));
        }
    }
}

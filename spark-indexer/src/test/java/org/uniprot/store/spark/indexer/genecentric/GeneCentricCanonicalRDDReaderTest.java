package org.uniprot.store.spark.indexer.genecentric;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import org.uniprot.store.spark.indexer.genecentric.mapper.FastaToProteomeGeneCount;
import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 20/10/2020
 */
class GeneCentricCanonicalRDDReaderTest {

    @Test
    void testLoadGeneCentricCanonicalProteins() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            GeneCentricCanonicalRDDReader reader = new GeneCentricCanonicalRDDReader(parameter);
            JavaPairRDD<String, GeneCentricEntry> uniprotRdd = reader.load();
            assertNotNull(uniprotRdd);

            assertEquals(40L, uniprotRdd.count());
            Tuple2<String, GeneCentricEntry> tuple =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("O51964")).first();

            assertNotNull(tuple);
            assertEquals("O51964", tuple._1);
            GeneCentricEntry entry = tuple._2;
            assertNotNull(entry);
            assertEquals("UP000000554", entry.getProteomeId());
            assertEquals("O51964_HALSA", entry.getCanonicalProtein().getUniProtkbId().getValue());

            tuple = uniprotRdd.filter(tuple2 -> tuple2._1.equals("A0A6G0Z640")).first();
            assertNotNull(tuple);
            assertEquals("A0A6G0Z640", tuple._1);
            entry = tuple._2;
            assertNotNull(entry);
            assertEquals("UP000478052", entry.getProteomeId());
            assertEquals(
                    "A0A6G0Z640_APHCR", entry.getCanonicalProtein().getUniProtkbId().getValue());
        }
    }

    @Test
    void loadProteomeGeneCounts() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            GeneCentricCanonicalRDDReader reader = new GeneCentricCanonicalRDDReader(parameter);

            JavaPairRDD<String, Integer> uniprotRdd = reader.loadProteomeGeneCounts();

            List<Tuple2<String, Integer>> collect = uniprotRdd.collect();
            assertThat(
                    collect,
                    containsInAnyOrder(
                            new Tuple2<>("UP000478052", 10), new Tuple2<>("UP000000554", 30)));
        }
    }

    @Test
    void loadProteomeGeneWithSplittedInputFileCounts() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                     SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            FakeSliptGeneCentricCanonicalRDDReader reader = new FakeSliptGeneCentricCanonicalRDDReader(parameter);

            JavaPairRDD<String, Integer> uniprotRdd = reader.loadProteomeGeneCounts();

            List<Tuple2<String, Integer>> collect = uniprotRdd.collect();
            assertThat(
                    collect,
                    containsInAnyOrder(
                            new Tuple2<>("UP000478052", 20), new Tuple2<>("UP000000554", 60)));
        }
    }

    private static class FakeSliptGeneCentricCanonicalRDDReader extends GeneCentricCanonicalRDDReader{

        public FakeSliptGeneCentricCanonicalRDDReader(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        protected <T> JavaPairRDD<String, T> loadWithMapper(Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, T>>> mapper) {
            return super.loadWithMapper(mapper).union(super.loadWithMapper(mapper));
        }
    }
}

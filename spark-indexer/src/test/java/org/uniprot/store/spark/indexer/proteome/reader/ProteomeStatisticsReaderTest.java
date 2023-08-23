package org.uniprot.store.spark.indexer.proteome.reader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

class ProteomeStatisticsReaderTest {
    private static JavaRDD<String> proteinRDD;
    private static final String RELEASE_NAME = "2020_02";

    @Test
    void getProteomeStatisticsRDD() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .sparkContext(sparkContext)
                            .applicationConfig(application)
                            .releaseName(RELEASE_NAME)
                            .build();
            proteinRDD = sparkContext.parallelize(getProteinString());
            ProteomeStatisticsReader reader = new ProteomeStatisticsReaderFake(jobParameter);

            JavaPairRDD<String, ProteomeStatistics> proteomeStatisticsRDD =
                    reader.getProteomeStatisticsRDD();

            List<Tuple2<String, ProteomeStatistics>> output = proteomeStatisticsRDD.collect();
            assertThat(
                    output,
                    containsInAnyOrder(
                            new Tuple2<>(
                                    "UP000005640",
                                    new ProteomeStatisticsBuilder()
                                            .reviewedProteinCount(2)
                                            .unreviewedProteinCount(0)
                                            .isoformProteinCount(1)
                                            .build()),
                            new Tuple2<>(
                                    "UP000005641",
                                    new ProteomeStatisticsBuilder()
                                            .reviewedProteinCount(1)
                                            .unreviewedProteinCount(0)
                                            .isoformProteinCount(0)
                                            .build()),
                            new Tuple2<>(
                                    "UP000005642",
                                    new ProteomeStatisticsBuilder()
                                            .reviewedProteinCount(0)
                                            .unreviewedProteinCount(1)
                                            .isoformProteinCount(0)
                                            .build())));
        }
    }

    private List<String> getProteinString() {
        return List.of(
                "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                        + "AC   A0-A0C5B5G6;\n"
                        + "Proteomes; UP000005640; Mitochondrion.\n"
                        + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                        + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.",
                "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                        + "AC   A0A0C5B5G7;\n"
                        + "Proteomes; UP000005640; Mitochondrion.\n"
                        + "Proteomes; UP000005641; Mitochondrion.\n"
                        + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                        + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.",
                "ID   MOTSC_HUMAN                       16 AA.\n"
                        + "AC   A0A0C5B5G8;\n"
                        + "Proteomes; UP000005642; Mitochondrion.\n"
                        + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                        + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.");
    }

    private static class ProteomeStatisticsReaderFake extends ProteomeStatisticsReader {

        public ProteomeStatisticsReaderFake(JobParameter parameter) {
            super(parameter);
        }

        @Override
        JavaRDD<String> getProteinInfo() {
            return proteinRDD;
        }
    }
}

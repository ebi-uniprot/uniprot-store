package org.uniprot.store.spark.indexer.proteome.mapper;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import scala.Tuple2;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

class ProteomeStatisticsMapperTest {
    private final ProteomeStatisticsMapper proteomeStatisticsMapper = new ProteomeStatisticsMapper();

    @Test
    void call_singleIsoform() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator = proteomeStatisticsMapper.call(getIsoformProtein());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(result, contains(new Tuple2<>("UP000005640", new ProteomeStatisticsBuilder().isoformProteinCount(1).build())));
    }

    @Test
    void call_multipleProteomes() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator = proteomeStatisticsMapper.call(getProteinWithTwoProteomes());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(result, contains(new Tuple2<>("UP000005640", new ProteomeStatisticsBuilder().reviewedProteinCount(1).build()),
                new Tuple2<>("UP000005641", new ProteomeStatisticsBuilder().reviewedProteinCount(1).build())));
    }

    @Test
    void call_singleUnreviewedProtein() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator = proteomeStatisticsMapper.call(getUnreviewedProtein());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(result, contains(new Tuple2<>("UP000005642", new ProteomeStatisticsBuilder().unreviewedProteinCount(1).build())));
    }

    private static String getUnreviewedProtein() {
        return "ID   MOTSC_HUMAN                       16 AA.\n"
                + "AC   A0A0C5B5G8;\n"
                + "Proteomes; UP000005642; Mitochondrion.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }

    private static String getProteinWithTwoProteomes() {
        return "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                + "AC   A0A0C5B5G7;\n"
                + "Proteomes; UP000005640; Mitochondrion.\n"
                + "Proteomes; UP000005641; Mitochondrion.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }

    private static String getIsoformProtein() {
        return "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                + "AC   A0-A0C5B5G6;\n"
                + "Proteomes; UP000005640; Mitochondrion.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }
}
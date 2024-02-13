package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;

import scala.Tuple2;

class ProteomeStatisticsMapperTest {
    private final ProteomeStatisticsMapper proteomeStatisticsMapper =
            new ProteomeStatisticsMapper();

    @Test
    void call_singleIsoform() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator =
                proteomeStatisticsMapper.call(getIsoformProtein());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(
                result,
                contains(
                        new Tuple2<>(
                                "UP000005640",
                                new ProteomeStatisticsBuilder().isoformProteinCount(1).build())));
    }

    @Test
    void call_multipleProteomes() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator =
                proteomeStatisticsMapper.call(getProteinWithTwoProteomes());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(
                result,
                contains(
                        new Tuple2<>(
                                "UP000005640",
                                new ProteomeStatisticsBuilder().reviewedProteinCount(1).build()),
                        new Tuple2<>(
                                "UP000005641",
                                new ProteomeStatisticsBuilder().reviewedProteinCount(1).build())));
    }

    @Test
    void call_multipleProteomesComponents() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator =
                proteomeStatisticsMapper.call(getProteinWithTwoProteomeComponents());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(
                result,
                contains(
                        new Tuple2<>(
                                "UP000005640",
                                new ProteomeStatisticsBuilder().reviewedProteinCount(1).build())));
    }

    @Test
    void call_singleUnreviewedProtein() {
        Iterator<Tuple2<String, ProteomeStatistics>> iterator =
                proteomeStatisticsMapper.call(getUnreviewedProtein());
        List<Tuple2<String, ProteomeStatistics>> result = new LinkedList<>();
        iterator.forEachRemaining(result::add);

        assertThat(
                result,
                contains(
                        new Tuple2<>(
                                "UP000005642",
                                new ProteomeStatisticsBuilder()
                                        .unreviewedProteinCount(1)
                                        .build())));
    }

    private static String getUnreviewedProtein() {
        return "ID   A0A0C5B5G8_9GEMI        Unreviewed;       121 AA.\n"
                + "AC   A0A0C5B5G8;\n"
                + "DR   Proteomes; UP000005642; Mitochondrion.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }

    private static String getProteinWithTwoProteomes() {
        return "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                + "AC   A0A0C5B5G7;\n"
                + "DR   Proteomes; UP000005640; Mitochondrion.\n"
                + "DR   Proteomes; UP000005641; Mitochondrion.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }

    private static String getProteinWithTwoProteomeComponents() {
        return "ID   MOTSC_HUMAN             Reviewed;          16 AA.\n"
                + "AC   A0A0C5B5G7;\n"
                + "DR   Proteomes; UP000005640; Mitochondrion 1.\n"
                + "DR   Proteomes; UP000005640; Mitochondrion 2.\n"
                + "CC   -!- FUNCTION: Regulates insulin sensitivity and metabolic homeostasis\n"
                + "CC       (PubMed:25738459, PubMed:33468709). Inhibits the folate cycle, thereby.";
    }

    private static String getIsoformProtein() {
        return "ID   FGFR2_HUMAN             Reviewed;         768 AA.\n"
                + "AC   P21802-2;\n"
                + "DR   Proteomes; UP000005640; Mitochondrion.\n"
                + "CC   -!- ALTERNATIVE PRODUCTS:\n"
                + "CC       Event=Alternative splicing; Named isoforms=17;\n"
                + "CC       Name=1; Synonyms=BEK, FGFR2IIIc;\n"
                + "CC         IsoId=P21802-1; Sequence=Displayed;\n"
                + "CC       Name=2; Synonyms=Short;\n"
                + "CC         IsoId=P21802-2; Sequence=VSP_002978;\n"
                + "CC       Name=3; Synonyms=BFR-1, FGFR2IIIb, KGFR;\n"
                + "CC         IsoId=P21802-3; Sequence=VSP_002969, VSP_002970, VSP_002971,\n"
                + "CC                                  VSP_002972;\n"
                + "CC       Name=4; Synonyms=K-sam;\n"
                + "CC         IsoId=P21802-4; Sequence=VSP_002964, VSP_002969, VSP_002970,\n"
                + "CC                                  VSP_002971, VSP_002972, VSP_002975,\n"
                + "CC                                  VSP_002976;\n"
                + "CC       Name=5; Synonyms=K-sam-I, BEK, IgIIIc;\n"
                + "CC         IsoId=P21802-5; Sequence=VSP_002975;\n"
                + "CC       Name=6; Synonyms=K-sam-IIC2;\n"
                + "CC         IsoId=P21802-6; Sequence=VSP_002975, VSP_002984;\n"
                + "CC       Name=7; Synonyms=K-sam-IIC3;\n"
                + "CC         IsoId=P21802-8; Sequence=VSP_002975, VSP_002978;\n"
                + "CC       Name=8; Synonyms=K-sam-IV, Soluble KGFR;\n"
                + "CC         IsoId=P21802-14; Sequence=VSP_002965, VSP_002966;\n"
                + "CC       Name=9; Synonyms=K-sam-III;\n"
                + "CC         IsoId=P21802-15; Sequence=VSP_002968;\n"
                + "CC       Name=10; Synonyms=TK14;\n"
                + "CC         IsoId=P21802-16; Sequence=VSP_002967, VSP_002975;\n"
                + "CC       Name=11;\n"
                + "CC         IsoId=P21802-17; Sequence=VSP_002969, VSP_002970, VSP_002971,\n"
                + "CC                                   VSP_002972, VSP_002978;\n"
                + "CC       Name=12; Synonyms=K-sam-IIC1, KGFR, IgIIIb;\n"
                + "CC         IsoId=P21802-18; Sequence=VSP_002969, VSP_002970, VSP_002971,\n"
                + "CC                                   VSP_002972, VSP_002975;\n"
                + "CC       Name=13; Synonyms=Soluble KGFR;\n"
                + "CC         IsoId=P21802-19; Sequence=VSP_002969, VSP_002970, VSP_002971,\n"
                + "CC                                   VSP_002972, VSP_002973, VSP_002974;\n"
                + "CC       Name=14;\n"
                + "CC         IsoId=P21802-20; Sequence=VSP_019608, VSP_019609;\n"
                + "CC       Name=15;\n"
                + "CC         IsoId=P21802-21; Sequence=VSP_002964, VSP_041915;\n"
                + "CC       Name=16;\n"
                + "CC         IsoId=P21802-22; Sequence=VSP_002964, VSP_002969, VSP_002970,\n"
                + "CC                                   VSP_002971, VSP_002972, VSP_002978;\n"
                + "CC       Name=17;\n"
                + "CC         IsoId=P21802-23; Sequence=VSP_041914;\n"
                + "CC   ---------------------------------------------------------------------------\n"
                + "CC   Copyrighted by the UniProt Consortium, see https://www.uniprot.org/terms\n"
                + "CC   Distributed under the Creative Commons Attribution (CC BY 4.0) License\n"
                + "CC   ---------------------------------------------------------------------------";
    }
}

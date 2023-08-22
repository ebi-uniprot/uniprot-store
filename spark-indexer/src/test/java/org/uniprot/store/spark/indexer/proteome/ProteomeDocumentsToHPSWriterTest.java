package org.uniprot.store.spark.indexer.proteome;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

class ProteomeDocumentsToHPSWriterTest {
    public static final String PROTEOME_ID_0 = "proteomeId0";
    public static final String PROTEOME_ID_1 = "proteomeId1";
    public static final String PROTEOME_ID_2 = "proteomeId2";
    public static final String[] PROTEOME_IDS = new String[]{PROTEOME_ID_0, PROTEOME_ID_1, PROTEOME_ID_2};
    public static final int ORGANISM_ID_0 = 19;
    public static final int ORGANISM_ID_1 = 333;
    public static final int ORGANISM_ID_2 = 67;
    public static final Integer[] ORGANISM_IDS = new Integer[]{ORGANISM_ID_0, ORGANISM_ID_1, ORGANISM_ID_2};
    public static final long REVIEWED_PROTEIN_COUNT_0 = 49;
    public static final long REVIEWED_PROTEIN_COUNT_1 = 53;
    public static final Long[] REVIEWED_PROTEIN_COUNTS = new Long[]{REVIEWED_PROTEIN_COUNT_0, REVIEWED_PROTEIN_COUNT_1, 0L};
    public static final long UNREVIEWED_PROTEIN_COUNT_0 = 5555;
    public static final long UNREVIEWED_PROTEIN_COUNT_1 = 10;
    public static final Long[] UNREVIEWED_PROTEIN_COUNTS = new Long[]{UNREVIEWED_PROTEIN_COUNT_0, UNREVIEWED_PROTEIN_COUNT_1, 0L};
    public static final long ISOFORM_PROTEIN_COUNT_0 = 36;
    public static final long ISOFORM_PROTEIN_COUNT_1 = 9998;
    public static final Long[] ISOFORM_PROTEIN_COUNTS = new Long[]{ISOFORM_PROTEIN_COUNT_0, ISOFORM_PROTEIN_COUNT_1, 0L};
    public static final String ORGANISM_SORT_0 = "organismSort0";
    public static final String ORGANISM_SORT_1 = "organismSort1";
    public static final String ORGANISM_SORT_2 = "organismSort2";
    public static final String[] ORGANISM_SORTS = new String[]{ORGANISM_SORT_0, ORGANISM_SORT_1, ORGANISM_SORT_2};
    private static final List<String> ORGANISM_NAME_0 = List.of("on0");
    private static final List<String> ORGANISM_NAME_1 = List.of("on1");
    private static final List<String> ORGANISM_NAME_2 = List.of("on2");
    private static final List<String>[] ORGANISM_NAMES = new List[]{ORGANISM_NAME_0, ORGANISM_NAME_1, ORGANISM_NAME_2};
    private static final List<String> ORGANISM_TAXON_0 = List.of("ot0");
    private static final List<String> ORGANISM_TAXON_1 = List.of("ot1");
    private static final List<String> ORGANISM_TAXON_2 = List.of("ot2");
    private static final List<String>[] ORGANISM_TAXONS = new List[]{ORGANISM_TAXON_0, ORGANISM_TAXON_1, ORGANISM_TAXON_2};
    private static final List<Integer> LINEAGE_ID_0 = List.of(10);
    private static final List<Integer> LINEAGE_ID_1 = List.of(11);
    private static final List<Integer> LINEAGE_ID_2 = List.of(12);
    private static final List<Integer>[] LINEAGE_IDS = new List[]{LINEAGE_ID_0, LINEAGE_ID_1, LINEAGE_ID_2};
    public static final String RELEASE_NAME = "23_03";
    private static JavaPairRDD<String, ProteomeEntry> proteomeRDD;
    private static JavaPairRDD<String, ProteomeStatistics> statisticsRDD;

    private static List<Tuple2<String, ProteomeStatistics>> getStatisticsTuples() {
        return List.of(
                new Tuple2<>(PROTEOME_ID_0, new ProteomeStatisticsBuilder().reviewedProteinCount(REVIEWED_PROTEIN_COUNT_0)
                        .unreviewedProteinCount(UNREVIEWED_PROTEIN_COUNT_0).isoformProteinCount(ISOFORM_PROTEIN_COUNT_0).build()),
                new Tuple2<>(PROTEOME_ID_1, new ProteomeStatisticsBuilder().reviewedProteinCount(REVIEWED_PROTEIN_COUNT_1)
                        .unreviewedProteinCount(UNREVIEWED_PROTEIN_COUNT_1).isoformProteinCount(ISOFORM_PROTEIN_COUNT_1).build())
        );
    }

    private static List<Tuple2<String, ProteomeEntry>> getProteomeTuples() {
        return List.of(
                new Tuple2<>(PROTEOME_ID_0, new ProteomeEntryBuilder().proteomeId(PROTEOME_ID_0).taxonomy(new TaxonomyBuilder().taxonId(ORGANISM_ID_0).build()).build()),
                new Tuple2<>(PROTEOME_ID_1, new ProteomeEntryBuilder().proteomeId(PROTEOME_ID_1).taxonomy(new TaxonomyBuilder().taxonId(ORGANISM_ID_1).build()).build()),
                new Tuple2<>(PROTEOME_ID_2, new ProteomeEntryBuilder().proteomeId(PROTEOME_ID_2).taxonomy(new TaxonomyBuilder().taxonId(ORGANISM_ID_2).build()).build())
        );
    }

    @Test
    void writeIndexDocumentsToHPS() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter jobParameter = JobParameter.builder().sparkContext(sparkContext).applicationConfig(application).releaseName(RELEASE_NAME).build();
            proteomeRDD = sparkContext.parallelizePairs(getProteomeTuples());
            statisticsRDD = sparkContext.parallelizePairs(getStatisticsTuples());
            ProteomeDocumentsToHPSWriterFake writer = new ProteomeDocumentsToHPSWriterFake(jobParameter);
            writer.writeIndexDocumentsToHPS();
            List<ProteomeDocument> savedDocuments = writer.savedDocuments;
            assertEquals(3, savedDocuments.size());
            int index = 0;
            for (ProteomeDocument savedDocument : savedDocuments) {
                assertResult(savedDocument, index);
                index++;
            }
        }
    }

    private void assertResult(ProteomeDocument proteomeDocument, int index) {
        assertEquals(PROTEOME_IDS[index], proteomeDocument.upid);
        assertEquals(REVIEWED_PROTEIN_COUNTS[index], proteomeDocument.reviewedProteinCount);
        assertEquals(UNREVIEWED_PROTEIN_COUNTS[index], proteomeDocument.unreviewedProteinCount);
        assertEquals(ISOFORM_PROTEIN_COUNTS[index], proteomeDocument.isoformProteinCount);
        assertEquals(ORGANISM_IDS[index], proteomeDocument.organismTaxId);
        assertEquals(ORGANISM_SORTS[index], proteomeDocument.organismSort);
        assertEquals(ORGANISM_TAXONS[index], proteomeDocument.organismTaxon);
        assertEquals(ORGANISM_NAMES[index], proteomeDocument.organismName);
        assertEquals(LINEAGE_IDS[index], proteomeDocument.taxLineageIds);
    }

    private static class ProteomeDocumentsToHPSWriterFake extends ProteomeDocumentsToHPSWriter {
        transient List<ProteomeDocument> savedDocuments;

        public ProteomeDocumentsToHPSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        Function<ProteomeEntry, ProteomeDocument> getEntryToProteomeDocumentMapper() {
            return proteomeEntry -> {
                ProteomeDocument proteomeDocument = new ProteomeDocument();
                proteomeDocument.upid = proteomeEntry.getId().getValue();
                proteomeDocument.organismTaxId = (int) proteomeEntry.getTaxonomy().getTaxonId();
                return proteomeDocument;
            };
        }

        @Override
        Function<Tuple2<ProteomeDocument, Optional<ProteomeStatistics>>, ProteomeDocument> getStatisticsToProteomeDocumentMapper() {
            return docStat -> {
                ProteomeDocument proteomeDocument = docStat._1;
                Optional<ProteomeStatistics> proteomeStatisticsOptional = docStat._2;

                if (proteomeStatisticsOptional.isPresent()) {
                    ProteomeStatistics proteomeStatistics = proteomeStatisticsOptional.get();
                    proteomeDocument.reviewedProteinCount = proteomeStatistics.getReviewedProteinCount();
                    proteomeDocument.unreviewedProteinCount = proteomeStatistics.getUnreviewedProteinCount();
                    proteomeDocument.isoformProteinCount = proteomeStatistics.getIsoformProteinCount();
                }

                return proteomeDocument;
            };
        }

        @Override
        Function<ProteomeDocument, ProteomeDocument> getTaxonomyToProteomeDocumentMapper() {
            return input -> {
                if (Objects.equals(ORGANISM_ID_0, input.organismTaxId)) {
                    input.organismSort = ORGANISM_SORT_0;
                    input.organismName = ORGANISM_NAME_0;
                    input.organismTaxon = ORGANISM_TAXON_0;
                    input.taxLineageIds = LINEAGE_ID_0;
                    return input;
                }
                if (Objects.equals(ORGANISM_ID_1, input.organismTaxId)) {
                    input.organismSort = ORGANISM_SORT_1;
                    input.organismName = ORGANISM_NAME_1;
                    input.organismTaxon = ORGANISM_TAXON_1;
                    input.taxLineageIds = LINEAGE_ID_1;
                    return input;
                }
                if (Objects.equals(ORGANISM_ID_2, input.organismTaxId)) {
                    input.organismSort = ORGANISM_SORT_2;
                    input.organismName = ORGANISM_NAME_2;
                    input.organismTaxon = ORGANISM_TAXON_2;
                    input.taxLineageIds = LINEAGE_ID_2;
                    return input;
                }
                throw new RuntimeException("Invalid input proteome to include tax data");
            };
        }

        @Override
        Map<String, TaxonomyEntry> getTaxonomyEntryMap() {
            return Map.of();
        }

        @Override
        JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
            return statisticsRDD;
        }

        @Override
        JavaPairRDD<String, ProteomeEntry> loadProteomeRDD() {
            return proteomeRDD;
        }

        @Override
        void saveToHPS(JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD) {
            this.savedDocuments = proteomeDocumentJavaRDD.collect();
        }
    }
}
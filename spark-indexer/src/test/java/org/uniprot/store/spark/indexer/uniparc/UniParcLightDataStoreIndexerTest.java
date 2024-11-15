package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder.HAS_ACTIVE_CROSS_REF;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.CommonOrganismBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class UniParcLightDataStoreIndexerTest {

    @Test
    void indexInDataStore() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            UniParcLightDataStoreIndexerTest.FakeUniParcLightDataStoreIndexer indexer =
                    new UniParcLightDataStoreIndexerTest.FakeUniParcLightDataStoreIndexer(
                            parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
            DataStoreParameter dataStoreParams =
                    indexer.getDataStoreParameter(parameter.getApplicationConfig());
            assertNotNull(dataStoreParams);
        }
    }

    private static class FakeUniParcLightDataStoreIndexer extends UniParcLightDataStoreIndexer {

        private final JobParameter jobParameter;

        public FakeUniParcLightDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
            this.jobParameter = jobParameter;
        }

        @Override
        void saveInDataStore(JavaRDD<UniParcEntryLight> uniParcJoinedRDD) {
            List<UniParcEntryLight> result = uniParcJoinedRDD.collect();
            assertNotNull(result);
            assertEquals(3, result.size());
            UniParcEntryLight entry1 = result.get(0);
            assertEquals("UPI00000E8551", entry1.getUniParcId());
            assertEquals(12, entry1.getCrossReferenceCount());
            assertTrue(entry1.getExtraAttributes().isEmpty());
            assertEquals(1, entry1.getCommonTaxons().size());
            assertEquals(
                    new CommonOrganismBuilder()
                            .topLevel("cellular organisms")
                            .commonTaxon("Teleostomi")
                            .commonTaxonId(117570L)
                            .build(),
                    entry1.getCommonTaxons().get(0));
            assertEquals(5, entry1.getUniProtKBAccessions().size());
            assertEquals(
                    Set.of("Q9EPI6", "Q9EPI6-1", "I8FBX0", "I8FBX2", "Q9EPI6.1"),
                    entry1.getUniProtKBAccessions());
            assertNotNull(entry1.getSequence());
            assertFalse(entry1.getSequenceFeatures().isEmpty());
            assertNotNull(entry1.getMostRecentCrossRefUpdated());
            assertEquals(LocalDate.of(2020, 4, 22), entry1.getMostRecentCrossRefUpdated());
            assertNotNull(entry1.getOldestCrossRefCreated());
            assertEquals(LocalDate.of(2001, 3, 1), entry1.getOldestCrossRefCreated());
            UniParcEntryLight entry2 = result.get(1);
            assertEquals("UPI000000017F", entry2.getUniParcId());
            assertEquals(2, entry2.getCommonTaxons().size());
            assertEquals(
                    new CommonOrganismBuilder()
                            .topLevel("cellular organisms")
                            .commonTaxon("Luzula")
                            .commonTaxonId(46322L)
                            .build(),
                    entry2.getCommonTaxons().get(0));
            assertEquals(
                    new CommonOrganismBuilder()
                            .topLevel("Viruses")
                            .commonTaxon("Nucleocytoviricota")
                            .commonTaxonId(35493L)
                            .build(),
                    entry2.getCommonTaxons().get(1));
            assertEquals(12, entry1.getCrossReferenceCount());
            assertTrue(entry2.getExtraAttributes().isEmpty());
            UniParcEntryLight entry3 = result.get(2);
            assertEquals("UPI0001C61C61", entry3.getUniParcId());
            assertEquals(1, entry3.getExtraAttributes().size());
            assertEquals(false, entry3.getExtraAttributes().get(HAS_ACTIVE_CROSS_REF));
        }

        @Override
        JavaPairRDD<String, List<TaxonomyLineage>> getTaxonomyWithLineageRDD() {
            // compute the lineage of the taxonomy ids in the format <2, <1,1315,2>> using db
            List<TaxonomyLineage> lineage1 = getLineageFor10116();
            List<TaxonomyLineage> lineage2 = getLineageFor117571();
            List<TaxonomyLineage> lineage3 = getLineageFor28141();
            List<TaxonomyLineage> lineage4 = getLineageFor1111();
            List<TaxonomyLineage> lineage5 = getLineageFor9986();
            Tuple2<String, List<TaxonomyLineage>> tuple1 = new Tuple2<>("10116", lineage1);
            Tuple2<String, List<TaxonomyLineage>> tuple2 = new Tuple2<>("117571", lineage2);
            Tuple2<String, List<TaxonomyLineage>> tuple3 = new Tuple2<>("28141", lineage3);
            Tuple2<String, List<TaxonomyLineage>> tuple4 = new Tuple2<>("1111", lineage4);
            Tuple2<String, List<TaxonomyLineage>> tuple5 = new Tuple2<>("9986", lineage5);
            return this.jobParameter
                    .getSparkContext()
                    .parallelizePairs(List.of(tuple1, tuple2, tuple3, tuple4, tuple5));
        }

        private List<TaxonomyLineage> getLineageFor9986() {
            Object[][] values = {
                {"cellular organisms", null, 131567, "no rank", true},
                {"Eukaryota", "eucaryotes", 2759, "superkingdom", false},
                {"Viridiplantae", null, 33090, "kingdom", false},
                {"Streptophyta", null, 35493, "phylum", false},
                {"Streptophytina", null, 131221, "subphylum", true}
            };
            return getTaxonomyLineages(values);
        }

        private List<TaxonomyLineage> getLineageFor1111() {
            Object[][] values = {
                {"Viruses", null, 10239, "no rank", true},
                {"Varidnaviria", "Varidnaviria", 2759, "superkingdom", false},
                {"Bamfordvirae", null, 33090, "kingdom", false},
                {"Nucleocytoviricota", null, 35493, "phylum", false}
            };
            return getTaxonomyLineages(values);
        }

        private List<TaxonomyLineage> getLineageFor28141() {
            Object[][] values = {
                {"cellular organisms", null, 131567, "no rank", true},
                {"Eukaryota", "eucaryotes", 2759, "superkingdom", false},
                {"Viridiplantae", null, 33090, "kingdom", false},
                {"Streptophyta", null, 35493, "phylum", false},
                {"Streptophytina", null, 131221, "subphylum", true},
                {"Embryophyta", null, 3193, "clade", false},
                {"Tracheophyta", null, 58023, "clade", false},
                {"Euphyllophyta", null, 78536, "clade", true},
                {"Spermatophyta", null, 58024, "clade", false},
                {"Magnoliopsida", "flowering plants", 3398, "class", false},
                {"Mesangiospermae", null, 1437183, "clade", true},
                {"Liliopsida", "monocotyledons", 4447, "clade", false},
                {"Petrosaviidae", null, 1437197, "subclass", true},
                {"commelinids", null, 4734, "clade", true},
                {"Poales", null, 38820, "order", false},
                {"Juncaceae", null, 14101, "family", false},
                {"Luzula", "woodrushes", 46322, "genus", false}
            };
            return getTaxonomyLineages(values);
        }

        List<TaxonomyLineage> getLineageFor117571() {
            // List of values
            Object[][] values = {
                {"cellular organisms", null, 131567, "no rank", true},
                {"Eukaryota", "eucaryotes", 2759, "superkingdom", false},
                {"Opisthokonta", null, 33154, "clade", true},
                {"Metazoa", "metazoans", 33208, "kingdom", false},
                {"Eumetazoa", null, 6072, "clade", true},
                {"Bilateria", null, 33213, "clade", true},
                {"Deuterostomia", null, 33511, "clade", true},
                {"Chordata", "chordates", 7711, "phylum", false},
                {"Craniata", null, 89593, "subphylum", false},
                {"Vertebrata", "vertebrates", 7742, "clade", false},
                {"Gnathostomata", "jawed vertebrates", 7776, "clade", true},
                {"Teleostomi", null, 117570, "clade", true}
            };
            return getTaxonomyLineages(values);
        }

        List<TaxonomyLineage> getLineageFor10116() {
            // List of values
            Object[][] values = {
                {"cellular organisms", null, 131567, "no rank", true},
                {"Eukaryota", "eucaryotes", 2759, "superkingdom", false},
                {"Opisthokonta", null, 33154, "clade", true},
                {"Metazoa", "metazoans", 33208, "kingdom", false},
                {"Eumetazoa", null, 6072, "clade", true},
                {"Bilateria", null, 33213, "clade", true},
                {"Deuterostomia", null, 33511, "clade", true},
                {"Chordata", "chordates", 7711, "phylum", false},
                {"Craniata", null, 89593, "subphylum", false},
                {"Vertebrata", "vertebrates", 7742, "clade", false},
                {"Gnathostomata", "jawed vertebrates", 7776, "clade", true},
                {"Teleostomi", null, 117570, "clade", true},
                {"Euteleostomi", "bony vertebrates", 117571, "clade", false},
                {"Sarcopterygii", null, 8287, "superclass", true},
                {"Dipnotetrapodomorpha", null, 1338369, "clade", true},
                {"Tetrapoda", "tetrapods", 32523, "clade", true},
                {"Amniota", "amniotes", 32524, "clade", true},
                {"Mammalia", "mammals", 40674, "class", false},
                {"Theria", null, 32525, "clade", true},
                {"Eutheria", "placentals", 9347, "clade", false},
                {"Boreoeutheria", null, 1437010, "clade", true},
                {"Euarchontoglires", null, 314146, "superorder", false},
                {"Glires", "Rodents and rabbits", 314147, "clade", false},
                {"Rodentia", "rodent", 9989, "order", false},
                {"Myomorpha", "mice and others", 1963758, "suborder", false},
                {"Muroidea", null, 337687, "clade", false},
                {"Muridae", null, 10066, "family", false},
                {"Murinae", null, 39107, "subfamily", false},
                {"Rattus", null, 10114, "genus", false}
            };
            return getTaxonomyLineages(values);
        }

        private List<TaxonomyLineage> getTaxonomyLineages(Object[][] values) {
            List<TaxonomyLineage> taxonomyLineages = new ArrayList<>();
            for (Object[] value : values) {
                TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
                builder.scientificName((String) value[0]);
                if (value[1] != null) {
                    builder.commonName((String) value[1]);
                }
                builder.taxonId((Integer) value[2]);
                builder.rank(TaxonomyRank.typeOf((String) value[3]));
                builder.hidden((Boolean) value[4]);
                TaxonomyLineage lineage = builder.build();
                taxonomyLineages.add(lineage);
            }
            return taxonomyLineages;
        }
    }
}

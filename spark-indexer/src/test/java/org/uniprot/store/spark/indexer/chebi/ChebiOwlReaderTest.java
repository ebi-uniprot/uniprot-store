package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

class ChebiOwlReaderTest {

    @Test
    void canLoadChebiEntriesFromOwlFile() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            ChebiOwlReader reader = new ChebiOwlReader(parameter);
            JavaPairRDD<Long, ChebiEntry> chebiRdd = reader.load();

            assertNotNull(chebiRdd);
            Map<Long, ChebiEntry> entries = chebiRdd.collectAsMap();
            assertEquals(28, entries.size());

            ChebiEntry carbonDioxide = entries.get(16526L);
            assertNotNull(carbonDioxide);
            assertEquals("16526", carbonDioxide.getId());
            assertEquals("carbon dioxide", carbonDioxide.getName());
            assertEquals("CURLTUGMZLYLDI-UHFFFAOYSA-N", carbonDioxide.getInchiKey());
            assertTrue(carbonDioxide.getSynonyms().contains("[CO2]"));
            assertTrue(carbonDioxide.getSynonyms().contains("CARBON DIOXIDE"));
            assertTrue(carbonDioxide.getSynonyms().contains("carbonic anhydride"));
            assertChebiIdsContain(carbonDioxide.getRelatedIds(), "138675");

            ChebiEntry gasMolecularEntity = entries.get(138675L);
            assertNotNull(gasMolecularEntity);
            assertChebiIdsContain(gasMolecularEntity.getMajorMicrospecies(), "4200");
        }
    }

    private static void assertChebiIdsContain(List<ChebiEntry> entries, String expectedId) {
        List<String> ids = entries.stream().map(ChebiEntry::getId).collect(Collectors.toList());
        assertTrue(ids.contains(expectedId));
    }
}

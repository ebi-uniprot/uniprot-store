package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class UniParcRDDTupleReaderTest {

    @Test
    void testLoadUniParc() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, true);
            JavaRDD<UniParcEntry> uniParcRdd = reader.load();
            assertNotNull(uniParcRdd);
            List<UniParcEntry> entries = uniParcRdd.collect();
            assertNotNull(entries);
            assertEquals(3, entries.size());
            List<String> ids =
                    entries.stream()
                            .map(entry -> entry.getUniParcId().getValue())
                            .collect(Collectors.toList());
            assertTrue(ids.contains("UPI00000E8551"));
            assertTrue(ids.contains("UPI000000017F"));
            assertTrue(ids.contains("UPI0001C61C61"));
        }
    }
}

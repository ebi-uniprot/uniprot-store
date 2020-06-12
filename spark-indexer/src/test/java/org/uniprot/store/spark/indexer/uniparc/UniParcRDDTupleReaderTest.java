package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 16/05/2020
 */
class UniParcRDDTupleReaderTest {

    @Test
    void testLoadUniParc() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniParcRDDTupleReader reader = new UniParcRDDTupleReader(parameter, true);
            JavaRDD<UniParcEntry> uniParcRdd = reader.load();
            assertNotNull(uniParcRdd);
            long count = uniParcRdd.count();
            assertEquals(1L, count);
            UniParcEntry entry = uniParcRdd.first();
            assertNotNull(entry);
            assertEquals("UPI00000E8551", entry.getUniParcId().getValue());
        }
    }
}

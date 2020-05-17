package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniref.UniRefRDDTupleReader;

import java.util.ResourceBundle;

import static org.junit.jupiter.api.Assertions.*;

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

            JavaRDD<UniParcEntry> uniParcRdd = UniParcRDDTupleReader.load(parameter, true);
            assertNotNull(uniParcRdd);
            long count = uniParcRdd.count();
            assertEquals(1L, count);
            UniParcEntry entry = uniParcRdd.first();
            assertNotNull(entry);
            assertEquals("UPI00000E8551", entry.getUniParcId().getValue());
        }
    }
}
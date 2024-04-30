package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class SubcellularLocationJoinMapperTest {

    @Test
    void testGetSubcellularLocationsFromUniProtEntry() throws Exception {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, true);
            JavaPairRDD<String, UniProtKBEntry> uniProtRDD = reader.load();
            assertNotNull(uniProtRDD);
            assertEquals(1L, uniProtRDD.count());
            Tuple2<String, UniProtKBEntry> tuple = uniProtRDD.first();

            SubcellularLocationJoinMapper mapper = new SubcellularLocationJoinMapper();
            Iterator<Tuple2<String, MappedProteinAccession>> subcellIterator = mapper.call(tuple);
            int subcellCount = 0;
            assertNotNull(subcellIterator);
            while (subcellIterator.hasNext()) {
                subcellCount++;
                Tuple2<String, MappedProteinAccession> slIdMappedProtein = subcellIterator.next();
                assertNotNull(slIdMappedProtein._2);
                assertEquals("Q9EPI6", slIdMappedProtein._2.getProteinAccession());
                assertTrue(slIdMappedProtein._2.isReviewed());
            }
            assertEquals(13, subcellCount++);
        }
    }
}

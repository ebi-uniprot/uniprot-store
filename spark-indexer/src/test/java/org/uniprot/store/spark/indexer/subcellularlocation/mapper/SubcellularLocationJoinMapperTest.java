package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Iterator;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class SubcellularLocationJoinMapperTest {

    @Test
    void testGetSubcellularLocationsFromUniProtEntry() throws Exception {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
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
            Iterator<Tuple2<String, Statistics>> subcellIterator = mapper.call(tuple);
            int subcellCount = 0;
            assertNotNull(subcellIterator);
            while (subcellIterator.hasNext()) {
                subcellCount++;
                Tuple2<String, Statistics> slIdStatsTuple = subcellIterator.next();
                assertNotNull(slIdStatsTuple._2);
                assertEquals(1L, slIdStatsTuple._2.getReviewedProteinCount());
                assertEquals(0L, slIdStatsTuple._2.getUnreviewedProteinCount());
            }
            assertEquals(12, subcellCount++);
        }
    }
}

package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.*;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 10/05/2020
 */
class InactiveUniProtKBRDDTupleReaderTest {

    @Test
    void testLoadInactiveUniProtKB() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            JavaPairRDD<String, UniProtKBEntry> uniprotRdd =
                    InactiveUniProtKBRDDTupleReader.load(parameter);
            assertNotNull(uniprotRdd);

            long count = uniprotRdd.count();
            assertEquals(23L, count);
            Tuple2<String, UniProtKBEntry> inactive =
                    uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX0")).first();
            validateDeletedInactiveEntry(inactive, "I8FBX0", null);

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX1")).first();
            validateDeletedInactiveEntry(inactive, "I8FBX1", DeletedReason.PROTEOME_REDUNDANCY);

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("I8FBX2")).first();
            validateDeletedInactiveEntry(inactive, "I8FBX2", null);

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00015")).first();
            validateMergedInactiveEntry(inactive, "Q00015", "P23141");

            inactive = uniprotRdd.filter(tuple2 -> tuple2._1.equals("Q00007")).first();
            validateDeMergedInactiveEntry(inactive, "Q00007", List.of("P63150", "P63151"));
        }
    }

    private static void validateDeletedInactiveEntry(
            Tuple2<String, UniProtKBEntry> deleted, String accession, DeletedReason deletedReason) {
        validateInactiveEntry(deleted, accession, InactiveReasonType.DELETED);
        UniProtKBEntry entry = deleted._2;
        assertEquals(deletedReason, entry.getInactiveReason().getDeletedReason());
    }

    private static void validateMergedInactiveEntry(
            Tuple2<String, UniProtKBEntry> deleted, String accession, String mergedTo) {
        validateInactiveEntry(deleted, accession, InactiveReasonType.MERGED);
        UniProtKBEntry entry = deleted._2;
        assertEquals(List.of(mergedTo), entry.getInactiveReason().getMergeDemergeTos());
    }

    private static void validateDeMergedInactiveEntry(
            Tuple2<String, UniProtKBEntry> deleted, String accession, List<String> demergedTo) {
        validateInactiveEntry(deleted, accession, InactiveReasonType.DEMERGED);
        UniProtKBEntry entry = deleted._2;
        assertEquals(demergedTo, entry.getInactiveReason().getMergeDemergeTos());
    }

    private static void validateInactiveEntry(
            Tuple2<String, UniProtKBEntry> inactive,
            String accession,
            InactiveReasonType reasonType) {
        assertNotNull(inactive);
        assertEquals(accession, inactive._1);
        UniProtKBEntry entry = inactive._2;
        assertEquals(UniProtKBEntryType.INACTIVE, entry.getEntryType());
        assertEquals(accession, entry.getPrimaryAccession().getValue());
        assertNotNull(entry.getInactiveReason());
        EntryInactiveReason reason = entry.getInactiveReason();
        assertEquals(reasonType, reason.getInactiveReasonType());
    }
}

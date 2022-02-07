//package org.uniprot.store.spark.indexer.subcellularlocation.mapper;
//
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.spark.api.java.Optional;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//import org.uniprot.core.Statistics;
//import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
//import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
//import org.uniprot.core.impl.StatisticsBuilder;
//
//import scala.Tuple2;
//
///**
// * @author sahmad
// * @created 03/02/2022
// */
//public class SubcellularLocationFlatAncestorTest {
//
//    @Test
//    void testAncestorFlattening() throws Exception {
//        SubcellularLocationFlatAncestor flatAncestor = new SubcellularLocationFlatAncestor();
//        SubcellularLocationEntry grandParent = createSubcellularLocationEntry("SL-0003");
//        SubcellularLocationEntry parent = createSubcellularLocationEntry("SL-0002");
//        SubcellularLocationEntry son = createSubcellularLocationEntry("SL-0001");
//        // create hierarchy
//        SubcellularLocationEntry parentWithParent =
//                SubcellularLocationEntryBuilder.from(parent).isAAdd(grandParent).build();
//        SubcellularLocationEntry sonWithParent =
//                SubcellularLocationEntryBuilder.from(son).isAAdd(parentWithParent).build();
//        Statistics modelStatistics =
//                new StatisticsBuilder().reviewedProteinCount(1L).unreviewedProteinCount(2L).build();
//        // get the flattened ancestors
//        Tuple2<SubcellularLocationEntry, Optional<Statistics>> tuple =
//                new Tuple2<>(sonWithParent, Optional.of(modelStatistics));
//        Iterator<Tuple2<String, SubcellularLocationEntry>> ancestorsIterator =
//                flatAncestor.call(tuple);
//        // verify the result
//        int ancestorsCount = 0;
//        while (ancestorsIterator.hasNext()) {
//            ancestorsCount++;
//            Tuple2<String, SubcellularLocationEntry> ancestor = ancestorsIterator.next();
//            verifyStatistics(ancestor, modelStatistics);
//        }
//        Assertions.assertEquals(3, ancestorsCount);
//    }
//
//    static SubcellularLocationEntry createSubcellularLocationEntry(String subcellId) {
//        SubcellularLocationEntryBuilder builder = new SubcellularLocationEntryBuilder();
//        builder.id(subcellId);
//        builder.name("name" + subcellId);
//        return builder.build();
//    }
//
//    private void verifyStatistics(
//            Tuple2<String, SubcellularLocationEntry> ancestor, Statistics modelStatistics) {
//        Statistics statistics = ancestor._2().getStatistics();
//        Assertions.assertTrue(
//                List.of("SL-0001", "SL-0002", "SL-0003").contains(ancestor._2.getId()));
//        Assertions.assertNotNull(statistics);
//        Assertions.assertEquals(
//                modelStatistics.getUnreviewedProteinCount(),
//                statistics.getUnreviewedProteinCount());
//        Assertions.assertEquals(
//                modelStatistics.getReviewedProteinCount(), statistics.getReviewedProteinCount());
//    }
//}

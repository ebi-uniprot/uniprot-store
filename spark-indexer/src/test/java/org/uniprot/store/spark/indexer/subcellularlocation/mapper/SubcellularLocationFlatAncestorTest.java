package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 03/02/2022
 */
public class SubcellularLocationFlatAncestorTest {

    @Test
    void testAncestorFlattening() throws Exception {
        SubcellularLocationFlatAncestor flatAncestor = new SubcellularLocationFlatAncestor();
        SubcellularLocationEntry grandParent = createSubcellularLocationEntry("SL-0003");
        SubcellularLocationEntry parent = createSubcellularLocationEntry("SL-0002");
        SubcellularLocationEntry son = createSubcellularLocationEntry("SL-0001");
        SubcellularLocationEntry partOf = createSubcellularLocationEntry("SL-0000");
        // create hierarchy
        /* ---> represents IS_A and ---- represents Part_Of
           1 ---> 2
           1 ---- 0
           2 ---->3
           0 -----3
           Note that 2 points to 3 and 0 points to 3 as well, it should be counted as one
        */
        SubcellularLocationEntry partOfSon =
                SubcellularLocationEntryBuilder.from(partOf).partOfAdd(grandParent).build();
        SubcellularLocationEntry parentWithParent =
                SubcellularLocationEntryBuilder.from(parent).isAAdd(grandParent).build();
        SubcellularLocationEntry sonWithParent =
                SubcellularLocationEntryBuilder.from(son)
                        .isAAdd(parentWithParent)
                        .partOfAdd(partOfSon)
                        .build();
        Statistics modelStatistics =
                new StatisticsBuilder().reviewedProteinCount(1L).unreviewedProteinCount(2L).build();
        List<MappedProteinAccession> mappedProteins = new ArrayList<>();
        List<String> proteins = List.of("P12345", "Q12345");
        MappedProteinAccession mpa1 =
                new MappedProteinAccession.MappedProteinAccessionBuilder()
                        .proteinAccession("P12345")
                        .isReviewed(true)
                        .build();
        MappedProteinAccession mpa2 =
                new MappedProteinAccession.MappedProteinAccessionBuilder()
                        .proteinAccession("Q12345")
                        .build();
        // get the flattened ancestors
        Tuple2<SubcellularLocationEntry, Optional<Iterable<MappedProteinAccession>>> tuple =
                new Tuple2<>(sonWithParent, Optional.of(List.of(mpa1, mpa2)));
        Iterator<Tuple2<String, Iterable<MappedProteinAccession>>> ancestorsIterator =
                flatAncestor.call(tuple);
        // verify the result
        int ancestorsCount = 0;
        while (ancestorsIterator.hasNext()) {
            ancestorsCount++;
            Tuple2<String, Iterable<MappedProteinAccession>> ancestor = ancestorsIterator.next();
            verifyStatistics(ancestor, proteins);
        }
        Assertions.assertEquals(4, ancestorsCount);
    }

    static SubcellularLocationEntry createSubcellularLocationEntry(String subcellId) {
        SubcellularLocationEntryBuilder builder = new SubcellularLocationEntryBuilder();
        builder.id(subcellId);
        builder.name("name" + subcellId);
        return builder.build();
    }

    private void verifyStatistics(
            Tuple2<String, Iterable<MappedProteinAccession>> ancestor, List<String> proteins) {
        List<String> accessions =
                StreamSupport.stream(ancestor._2().spliterator(), false)
                        .map(pa -> pa.getProteinAccession())
                        .collect(Collectors.toList());
        Assertions.assertTrue(
                List.of("SL-0001", "SL-0002", "SL-0003", "SL-0000").contains(ancestor._1));
        Assertions.assertEquals(accessions.size(), proteins.size());
        Assertions.assertEquals(accessions.toString(), proteins.toString());
    }
}

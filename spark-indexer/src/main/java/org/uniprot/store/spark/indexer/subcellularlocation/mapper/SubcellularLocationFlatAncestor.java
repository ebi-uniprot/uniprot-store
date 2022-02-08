package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 31/01/2022
 */
public class SubcellularLocationFlatAncestor
        implements PairFlatMapFunction<
                Tuple2<SubcellularLocationEntry, Optional<Iterable<MappedProteinAccession>>>,
                String,
                Iterable<MappedProteinAccession>> {

    @Override
    public Iterator<Tuple2<String, Iterable<MappedProteinAccession>>> call(
            Tuple2<SubcellularLocationEntry, Optional<Iterable<MappedProteinAccession>>> tuple)
            throws Exception {
        List<Tuple2<String, Iterable<MappedProteinAccession>>> relatedTuples = new ArrayList<>();
        if (tuple._2.isPresent()) { // if proteins are there
            Iterable<MappedProteinAccession> proteinAccessions = tuple._2.get();
            SubcellularLocationEntry currentEntry = tuple._1;
            Set<String> allRelatedNodes = new HashSet<>();
            traverseRelatedEntries(currentEntry, allRelatedNodes);
            relatedTuples = percolateDownAccessions(proteinAccessions, allRelatedNodes);
        } else {
            relatedTuples.add(createTuple2(tuple._1));
        }
        return relatedTuples.iterator();
    }

    private void traverseRelatedEntries(
            SubcellularLocationEntry currentEntry, Set<String> allRelatedEntries) {
        if (!allRelatedEntries.contains(currentEntry.getId())) {
            allRelatedEntries.add(currentEntry.getId());
            List<SubcellularLocationEntry> parents = currentEntry.getIsA();
            List<SubcellularLocationEntry> partOfs = currentEntry.getPartOf();
            List<SubcellularLocationEntry> relatedEntries = new ArrayList<>(parents);
            relatedEntries.addAll(partOfs);
            for (SubcellularLocationEntry related : relatedEntries) {
                traverseRelatedEntries(related, allRelatedEntries);
            }
        }
    }

    private List<Tuple2<String, Iterable<MappedProteinAccession>>> percolateDownAccessions(
            Iterable<MappedProteinAccession> proteinAccessions, Set<String> allRelatedEntries) {
        List<Tuple2<String, Iterable<MappedProteinAccession>>> selfAncestors = new ArrayList<>();
        for (String currentNode : allRelatedEntries) {
            Tuple2<String, Iterable<MappedProteinAccession>> tuple =
                    new Tuple2<>(currentNode, proteinAccessions);
            selfAncestors.add(tuple);
        }

        return selfAncestors;
    }

    private Tuple2<String, Iterable<MappedProteinAccession>> createTuple2(
            SubcellularLocationEntry entry) {
        return new Tuple2<>(entry.getId(), new ArrayList<>());
    }
}

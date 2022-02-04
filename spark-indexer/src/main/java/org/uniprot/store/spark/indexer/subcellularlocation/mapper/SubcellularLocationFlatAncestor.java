package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 31/01/2022
 */
public class SubcellularLocationFlatAncestor
        implements PairFlatMapFunction<
                Tuple2<SubcellularLocationEntry, Optional<Statistics>>,
                String,
                SubcellularLocationEntry> {

    @Override
    public Iterator<Tuple2<String, SubcellularLocationEntry>> call(
            Tuple2<SubcellularLocationEntry, Optional<Statistics>> tuple) throws Exception {
        List<Tuple2<String, SubcellularLocationEntry>> relatedTuples = new ArrayList<>();
        if (tuple._2.isPresent()) { // if stats is there
            Statistics statistics = tuple._2.get();
            SubcellularLocationEntry currentEntry = tuple._1;
            List<SubcellularLocationEntry> allRelatedNodes = new ArrayList<>();
            traverseRelatedEntries(currentEntry, allRelatedNodes);
            relatedTuples = updateStatistics(statistics, allRelatedNodes);
        } else {
            relatedTuples.add(createTuple2(tuple._1));
        }
        return relatedTuples.iterator();
    }

    private void traverseRelatedEntries(SubcellularLocationEntry currentEntry, List<SubcellularLocationEntry> allRelatedEntries){
        if(!isProcessed(currentEntry, allRelatedEntries)){
            allRelatedEntries.add(currentEntry);
            List<SubcellularLocationEntry> parents = currentEntry.getIsA();
            List<SubcellularLocationEntry> partOfs = currentEntry.getPartOf();
            List<SubcellularLocationEntry> relatedEntries = new ArrayList<>(parents);
            relatedEntries.addAll(partOfs);
            for (SubcellularLocationEntry related : relatedEntries) {
                traverseRelatedEntries(related, allRelatedEntries);
            }
        }
    }

    private List<Tuple2<String, SubcellularLocationEntry>> updateStatistics(Statistics statistics,
                                                                            List<SubcellularLocationEntry> allRelatedEntries) {
        List<Tuple2<String, SubcellularLocationEntry>> selfAncestors = new ArrayList<>();
        for(SubcellularLocationEntry currentNode : allRelatedEntries) {
            SubcellularLocationEntryBuilder builder = SubcellularLocationEntryBuilder.from(currentNode);
            Statistics currentNodeStats = getCurrentNodeStats(currentNode);
            Statistics mergedStats = mergeStatistics(currentNodeStats, statistics);
            builder.statistics(mergedStats);
            selfAncestors.add(createTuple2(builder.build()));
        }

        return selfAncestors;
    }

    private boolean isProcessed(SubcellularLocationEntry currrentNode, List<SubcellularLocationEntry> processedNodes){
        return processedNodes.stream().anyMatch(node -> currrentNode.getId().equals(node.getId()));
    }

    private Tuple2<String, SubcellularLocationEntry> createTuple2(SubcellularLocationEntry entry) {
        return new Tuple2<>(entry.getId(), entry);
    }

    private Statistics getCurrentNodeStats(SubcellularLocationEntry currentNode) {
        if (Objects.isNull(currentNode.getStatistics())) {
            return new StatisticsBuilder().build();
        }
        return currentNode.getStatistics();
    }

    private Statistics mergeStatistics(Statistics currentNodeStats, Statistics modelStats) {
        return new StatisticsBuilder()
                .reviewedProteinCount(
                        currentNodeStats.getReviewedProteinCount()
                                + modelStats.getReviewedProteinCount())
                .unreviewedProteinCount(
                        currentNodeStats.getUnreviewedProteinCount()
                                + modelStats.getUnreviewedProteinCount())
                .build();
    }
}

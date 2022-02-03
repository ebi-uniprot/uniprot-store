package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
        List<Tuple2<String, SubcellularLocationEntry>> selfAncestors = new ArrayList<>();
        if (tuple._2.isPresent()) {
            Statistics stats = tuple._2.get();
            SubcellularLocationEntry node = tuple._1;
            traverseParents(node, stats, selfAncestors);
        } else {
            selfAncestors.add(createTuple2(tuple._1));
        }
        return selfAncestors.iterator();
    }

    private void traverseParents(
            SubcellularLocationEntry currentNode,
            Statistics statistics,
            List<Tuple2<String, SubcellularLocationEntry>> selfAncestors) {
        SubcellularLocationEntryBuilder builder = SubcellularLocationEntryBuilder.from(currentNode);
        Statistics currentNodeStats = getCurrentNodeStats(currentNode);
        Statistics mergedStats = mergeStatistics(currentNodeStats, statistics);
        builder.statistics(mergedStats);
        selfAncestors.add(createTuple2(builder.build()));

        List<SubcellularLocationEntry> parents = currentNode.getIsA();
        for (SubcellularLocationEntry parent : parents) {
            traverseParents(parent, statistics, selfAncestors);
        }
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

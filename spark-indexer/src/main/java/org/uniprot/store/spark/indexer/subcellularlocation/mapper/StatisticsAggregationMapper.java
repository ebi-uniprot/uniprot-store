package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.Statistics;
import org.uniprot.core.impl.StatisticsBuilder;

import java.util.HashSet;

/**
 * @author sahmad
 * @created 02/02/2022
 */
public class StatisticsAggregationMapper implements Function<HashSet<MappedProteinAccession>, Statistics> {

    @Override
    public Statistics call(HashSet<MappedProteinAccession> input) throws Exception {
        long reviewedCount = input.stream().filter(MappedProteinAccession::isReviewed).count();
        long unreviewedCount = input.stream().filter(acc -> !acc.isReviewed()).count();
        return new StatisticsBuilder().reviewedProteinCount(reviewedCount).unreviewedProteinCount(unreviewedCount).build();
    }
}

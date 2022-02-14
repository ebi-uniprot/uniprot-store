package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.HashSet;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.Statistics;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.core.util.Utils;

/**
 * @author sahmad
 * @created 02/02/2022
 */
public class StatisticsAggregationMapper
        implements Function<HashSet<MappedProteinAccession>, Statistics> {

    @Override
    public Statistics call(HashSet<MappedProteinAccession> input) throws Exception {
        long reviewedCount =
                input.stream()
                        .filter(
                                mpa ->
                                        Utils.notNullNotEmpty(mpa.getProteinAccession())
                                                && mpa.isReviewed())
                        .count();
        long unreviewedCount =
                input.stream()
                        .filter(
                                mpa ->
                                        Utils.notNullNotEmpty(mpa.getProteinAccession())
                                                && !mpa.isReviewed())
                        .count();
        return new StatisticsBuilder()
                .reviewedProteinCount(reviewedCount)
                .unreviewedProteinCount(unreviewedCount)
                .build();
    }
}

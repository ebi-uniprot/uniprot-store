package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.Statistics;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author sahmad
 * @created 02/02/2022
 */
public class StatisticsAggregationMapper implements Function2<Statistics, Statistics, Statistics> {
    @Override
    public Statistics call(Statistics stat1, Statistics stat2) throws Exception {
        Statistics mergedStats;
        if (SparkUtils.isThereAnyNullEntry(stat1, stat2)) {
            mergedStats = SparkUtils.getNotNullEntry(stat1, stat2);
        } else {
            mergedStats =
                    new StatisticsBuilder()
                            .reviewedProteinCount(
                                    stat1.getReviewedProteinCount()
                                            + stat2.getReviewedProteinCount())
                            .unreviewedProteinCount(
                                    stat1.getUnreviewedProteinCount()
                                            + stat2.getUnreviewedProteinCount())
                            .build();
        }
        return mergedStats;
    }
}

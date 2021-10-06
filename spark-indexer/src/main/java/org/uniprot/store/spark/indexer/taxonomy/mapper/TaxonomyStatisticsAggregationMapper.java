package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

public class TaxonomyStatisticsAggregationMapper
        implements Function2<TaxonomyStatistics, TaxonomyStatistics, TaxonomyStatistics> {

    private static final long serialVersionUID = -2085055845000585882L;

    @Override
    public TaxonomyStatistics call(TaxonomyStatistics stat1, TaxonomyStatistics stat2)
            throws Exception {
        TaxonomyStatistics mergedStat = null;
        if (SparkUtils.isThereAnyNullEntry(stat1, stat2)) {
            mergedStat = SparkUtils.getNotNullEntry(stat1, stat2);
        } else {
            mergedStat =
                    new TaxonomyStatisticsBuilder()
                            .reviewedProteinCount(
                                    stat1.getReviewedProteinCount()
                                            + stat2.getReviewedProteinCount())
                            .unreviewedProteinCount(
                                    stat1.getUnreviewedProteinCount()
                                            + stat2.getUnreviewedProteinCount())
                            .proteomeCount(stat1.getProteomeCount() + stat2.getProteomeCount())
                            .referenceProteomeCount(
                                    stat1.getReferenceProteomeCount()
                                            + stat2.getReferenceProteomeCount())
                            .build();
        }
        return mergedStat;
    }
}

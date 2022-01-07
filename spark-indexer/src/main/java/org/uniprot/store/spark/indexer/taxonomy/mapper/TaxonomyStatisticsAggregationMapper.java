package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

public class TaxonomyStatisticsAggregationMapper
        implements Function2<
                TaxonomyStatisticsWrapper, TaxonomyStatisticsWrapper, TaxonomyStatisticsWrapper> {

    private static final long serialVersionUID = -2085055845000585882L;

    @Override
    public TaxonomyStatisticsWrapper call(
            TaxonomyStatisticsWrapper wrapper1, TaxonomyStatisticsWrapper wrapper2)
            throws Exception {
        TaxonomyStatisticsWrapper mergedWrapper = null;
        if (SparkUtils.isThereAnyNullEntry(wrapper1, wrapper2)) {
            mergedWrapper = SparkUtils.getNotNullEntry(wrapper1, wrapper2);
        } else {
            TaxonomyStatistics stat1 = wrapper1.getStatistics();
            TaxonomyStatistics stat2 = wrapper2.getStatistics();
            TaxonomyStatistics mergedStat =
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
            mergedWrapper =
                    TaxonomyStatisticsWrapper.builder()
                            .statistics(mergedStat)
                            .organismReviewedProtein(
                                    wrapper1.isOrganismReviewedProtein()
                                            || wrapper2.isOrganismReviewedProtein())
                            .organismUnreviewedProtein(
                                    wrapper1.isOrganismUnreviewedProtein()
                                            || wrapper2.isOrganismUnreviewedProtein())
                            .build();
        }
        return mergedWrapper;
    }
}

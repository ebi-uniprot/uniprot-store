package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;

public class ProteomeStatisticsAggregationMapper
        implements Function2<ProteomeStatistics, ProteomeStatistics, ProteomeStatistics> {
    private static final long serialVersionUID = 3452201288759365796L;

    @Override
    public ProteomeStatistics call(
            ProteomeStatistics proteomeStatistics1, ProteomeStatistics proteomeStatistics2)
            throws Exception {
        return new ProteomeStatisticsBuilder()
                .reviewedProteinCount(
                        proteomeStatistics1.getReviewedProteinCount()
                                + proteomeStatistics2.getReviewedProteinCount())
                .unreviewedProteinCount(
                        proteomeStatistics1.getUnreviewedProteinCount()
                                + proteomeStatistics2.getUnreviewedProteinCount())
                .isoformProteinCount(
                        proteomeStatistics1.getIsoformProteinCount()
                                + proteomeStatistics2.getIsoformProteinCount())
                .build();
    }
}

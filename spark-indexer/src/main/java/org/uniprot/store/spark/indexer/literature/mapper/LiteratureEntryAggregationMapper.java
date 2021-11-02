package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
public class LiteratureEntryAggregationMapper
        implements Function2<LiteratureEntry, LiteratureEntry, LiteratureEntry> {
    private static final long serialVersionUID = -3841499052452967811L;

    @Override
    public LiteratureEntry call(LiteratureEntry entry1, LiteratureEntry entry2) throws Exception {
        LiteratureEntry mergedEntry = null;
        if (SparkUtils.isThereAnyNullEntry(entry1, entry2)) {
            mergedEntry = SparkUtils.getNotNullEntry(entry1, entry2);
        } else {
            LiteratureStatistics mergedStats = mergeStatistics(entry1, entry2);

            mergedEntry = LiteratureEntryBuilder.from(entry1).statistics(mergedStats).build();
        }
        return mergedEntry;
    }

    private LiteratureStatistics mergeStatistics(LiteratureEntry entry1, LiteratureEntry entry2) {
        long reviewed =
                entry1.getStatistics().getReviewedProteinCount()
                        + entry2.getStatistics().getReviewedProteinCount();

        long unreviewed =
                entry1.getStatistics().getUnreviewedProteinCount()
                        + entry2.getStatistics().getUnreviewedProteinCount();

        return new LiteratureStatisticsBuilder()
                .reviewedProteinCount(reviewed)
                .unreviewedProteinCount(unreviewed)
                .build();
    }
}

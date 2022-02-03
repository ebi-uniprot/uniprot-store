package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.util.Objects;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author sahmad
 * @created 02/02/2022
 */
public class SubcellullarLocationStatisticsMerger
        implements Function2<
                SubcellularLocationEntry, SubcellularLocationEntry, SubcellularLocationEntry> {
    @Override
    public SubcellularLocationEntry call(SubcellularLocationEntry sl1, SubcellularLocationEntry sl2)
            throws Exception {
        SubcellularLocationEntry mergedEntry;
        if (SparkUtils.isThereAnyNullEntry(sl1, sl2)) {
            mergedEntry = SparkUtils.getNotNullEntry(sl1, sl2);
        } else {
            Statistics stat1 = getStatistics(sl1);
            Statistics stat2 = getStatistics(sl2);
            Statistics mergedStats =
                    new StatisticsBuilder()
                            .reviewedProteinCount(
                                    stat1.getReviewedProteinCount()
                                            + stat2.getReviewedProteinCount())
                            .unreviewedProteinCount(
                                    stat1.getUnreviewedProteinCount()
                                            + stat2.getUnreviewedProteinCount())
                            .build();
            mergedEntry = SubcellularLocationEntryBuilder.from(sl1).statistics(mergedStats).build();
        }
        return mergedEntry;
    }

    private Statistics getStatistics(SubcellularLocationEntry sl) {
        if (Objects.isNull(sl.getStatistics())) {
            return new StatisticsBuilder().build();
        }

        return sl.getStatistics();
    }
}

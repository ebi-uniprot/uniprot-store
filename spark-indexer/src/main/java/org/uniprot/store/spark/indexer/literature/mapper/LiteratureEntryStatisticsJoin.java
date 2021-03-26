package org.uniprot.store.spark.indexer.literature.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/03/2021
 */
public class LiteratureEntryStatisticsJoin
        implements Function<Tuple2<LiteratureEntry, Optional<Long>>, LiteratureEntry> {
    private static final long serialVersionUID = 4504287812636452983L;

    public enum StatisticsType {
        COMMUNITY,
        COMPUTATIONALLY
    }

    private final StatisticsType statisticsType;

    public LiteratureEntryStatisticsJoin(StatisticsType statisticsType) {
        this.statisticsType = statisticsType;
    }

    @Override
    public LiteratureEntry call(Tuple2<LiteratureEntry, Optional<Long>> tuple) throws Exception {
        LiteratureEntry result = tuple._1;
        if (tuple._2.isPresent()) {
            LiteratureEntryBuilder builder = LiteratureEntryBuilder.from(result);
            LiteratureStatisticsBuilder statsBuilder =
                    LiteratureStatisticsBuilder.from(result.getStatistics());
            if (statisticsType == StatisticsType.COMMUNITY) {
                statsBuilder.communityMappedProteinCount(tuple._2.get());
            } else {
                statsBuilder.computationallyMappedProteinCount(tuple._2.get());
            }
            result = builder.statistics(statsBuilder.build()).build();
        }
        return result;
    }
}

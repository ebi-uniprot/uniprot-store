package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import scala.Tuple2;

public class ProteomeStatisticsToProteomeEntryMapper implements Function<
        Tuple2<ProteomeEntry, Optional<ProteomeStatistics>>, ProteomeEntry> {
    private static final long serialVersionUID = -8811772092559292338L;

    @Override
    public ProteomeEntry call(Tuple2<ProteomeEntry, Optional<ProteomeStatistics>> proteomeEntryProteomeStatisticsTuple2) throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryProteomeStatisticsTuple2._1;
        ProteomeStatistics proteomeStatistics = proteomeEntryProteomeStatisticsTuple2._2.orElse(
                new ProteomeStatisticsBuilder().reviewedProteinCount(0).unreviewedProteinCount(0).isoformProteinCount(0).build());

        ProteomeEntryBuilder proteomeEntryBuilder = ProteomeEntryBuilder.from(proteomeEntry);
        proteomeEntryBuilder.proteomeStatistics(proteomeStatistics);

        return proteomeEntryBuilder.build();
    }
}

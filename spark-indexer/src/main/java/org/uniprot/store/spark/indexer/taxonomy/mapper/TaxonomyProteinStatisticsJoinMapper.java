package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import scala.Tuple2;

public class TaxonomyProteinStatisticsJoinMapper
        implements Function<Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>>, TaxonomyEntry> {
    private static final long serialVersionUID = 4706545375756650869L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple)
            throws Exception {
        TaxonomyEntry result = tuple._1;
        if (tuple._2.isPresent()) {
            TaxonomyStatisticsBuilder statisticsBuilder =
                    TaxonomyStatisticsBuilder.from(tuple._2.get());

            TaxonomyEntryBuilder entryBuilder = TaxonomyEntryBuilder.from(result);
            if (result.hasStatistics()) {
                statisticsBuilder.proteomeCount(result.getStatistics().getProteomeCount());
                statisticsBuilder.referenceProteomeCount(
                        result.getStatistics().getReferenceProteomeCount());
            }
            entryBuilder.statistics(statisticsBuilder.build());
            result = entryBuilder.build();
        }
        return result;
    }
}

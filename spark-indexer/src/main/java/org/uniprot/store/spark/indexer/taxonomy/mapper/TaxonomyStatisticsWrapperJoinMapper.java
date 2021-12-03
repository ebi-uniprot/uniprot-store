package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

public class TaxonomyStatisticsWrapperJoinMapper
        implements Function<
                Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>>,
                TaxonomyStatisticsWrapper> {

    private static final long serialVersionUID = 7240080849019578656L;

    @Override
    public TaxonomyStatisticsWrapper call(
            Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>> tuple)
            throws Exception {
        TaxonomyStatisticsWrapper.TaxonomyStatisticsWrapperBuilder builder;
        if (tuple._1.isPresent()) {
            builder = tuple._1.get().toBuilder();

            if (tuple._2.isPresent()) {
                TaxonomyStatistics proteomeStats = tuple._2.get().getStatistics();
                TaxonomyStatisticsBuilder statisticsBuilder =
                        TaxonomyStatisticsBuilder.from(tuple._1.get().getStatistics());

                statisticsBuilder.proteomeCount(proteomeStats.getProteomeCount());
                statisticsBuilder.referenceProteomeCount(proteomeStats.getReferenceProteomeCount());

                builder.statistics(statisticsBuilder.build());
            }
        } else if (tuple._2.isPresent()) {
            builder = tuple._2.get().toBuilder();
        } else {
            builder =
                    TaxonomyStatisticsWrapper.builder()
                            .statistics(new TaxonomyStatisticsBuilder().build());
        }
        return builder.build();
    }
}

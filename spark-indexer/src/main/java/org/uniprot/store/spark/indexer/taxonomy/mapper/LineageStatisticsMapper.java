package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

public class LineageStatisticsMapper
        implements PairFlatMapFunction<
                Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>>,
                String,
                TaxonomyStatisticsWrapper> {

    private static final long serialVersionUID = 5593935136483975955L;

    @Override
    public Iterator<Tuple2<String, TaxonomyStatisticsWrapper>> call(
            Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple) throws Exception {
        TaxonomyStatisticsWrapper.TaxonomyStatisticsWrapperBuilder builder;
        if (tuple._2.isPresent()) {
            builder = tuple._2.get().toBuilder();
        } else {
            builder =
                    TaxonomyStatisticsWrapper.builder()
                            .statistics(new TaxonomyStatisticsBuilder().build());
        }
        List<Tuple2<String, TaxonomyStatisticsWrapper>> result = new ArrayList<>();
        String taxonomyId = String.valueOf(tuple._1.getTaxonId());
        result.add(new Tuple2<>(taxonomyId, builder.build()));

        builder.organismReviewedProtein(false);
        builder.organismUnreviewedProtein(false);
        final TaxonomyStatisticsWrapper lineageStat = builder.build();

        result.addAll(
                tuple._1.getLineages().stream()
                        .map(TaxonomyLineage::getTaxonId)
                        .map(String::valueOf)
                        .map(taxId -> new Tuple2<>(taxId, lineageStat))
                        .collect(Collectors.toList()));

        return result.iterator();
    }
}

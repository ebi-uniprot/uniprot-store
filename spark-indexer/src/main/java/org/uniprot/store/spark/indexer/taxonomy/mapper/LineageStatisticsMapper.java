package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import scala.Tuple2;

public class LineageStatisticsMapper
        implements PairFlatMapFunction<
                Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>>, String, TaxonomyStatistics> {

    private static final long serialVersionUID = 5593935136483975955L;

    @Override
    public Iterator<Tuple2<String, TaxonomyStatistics>> call(
            Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple) throws Exception {
        final TaxonomyStatistics statistics;
        if (tuple._2.isPresent()) {
            statistics = tuple._2.get();
        } else {
            statistics = new TaxonomyStatisticsBuilder().build();
        }
        List<Tuple2<String, TaxonomyStatistics>> result = new ArrayList<>();
        String taxonomyId = String.valueOf(tuple._1.getTaxonId());
        result.add(new Tuple2<>(taxonomyId, statistics));

        result.addAll(
                tuple._1.getLineages().stream()
                        .map(TaxonomyLineage::getTaxonId)
                        .map(String::valueOf)
                        .map(taxId -> new Tuple2<>(taxId, statistics))
                        .collect(Collectors.toList()));

        return result.iterator();
    }
}

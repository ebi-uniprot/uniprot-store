package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeType;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import scala.Tuple2;

public class ProteomeTaxonomyStatisticsMapper
        implements PairFunction<ProteomeEntry, String, TaxonomyStatistics> {
    @Override
    public Tuple2<String, TaxonomyStatistics> call(ProteomeEntry entry) throws Exception {
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if (entry.getProteomeType().equals(ProteomeType.REFERENCE)) {
            statisticsBuilder.referenceProteomeCount(1);
        }
        statisticsBuilder.proteomeCount(1);
        String taxonomyId = String.valueOf(entry.getTaxonomy().getTaxonId());
        return new Tuple2<>(taxonomyId, statisticsBuilder.build());
    }
}

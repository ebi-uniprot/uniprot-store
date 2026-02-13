package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.uniprot.core.proteome.ProteomeType.REFERENCE;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

public class ProteomeTaxonomyStatisticsMapper
        implements PairFunction<ProteomeEntry, String, TaxonomyStatisticsWrapper> {
    private static final long serialVersionUID = -4013227058179840505L;

    @Override
    public Tuple2<String, TaxonomyStatisticsWrapper> call(ProteomeEntry entry) throws Exception {
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if (REFERENCE.equals(entry.getProteomeType())) {
            statisticsBuilder.referenceProteomeCount(1);
        }
        statisticsBuilder.proteomeCount(1);
        TaxonomyStatisticsWrapper statisticsWrapper =
                TaxonomyStatisticsWrapper.builder().statistics(statisticsBuilder.build()).build();
        String taxonomyId = String.valueOf(entry.getTaxonomy().getTaxonId());
        return new Tuple2<>(taxonomyId, statisticsWrapper);
    }
}

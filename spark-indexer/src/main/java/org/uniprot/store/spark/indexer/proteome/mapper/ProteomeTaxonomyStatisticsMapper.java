package org.uniprot.store.spark.indexer.proteome.mapper;

import java.util.EnumSet;
import java.util.Set;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeType;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

public class ProteomeTaxonomyStatisticsMapper
        implements PairFunction<ProteomeEntry, String, TaxonomyStatisticsWrapper> {
    private static final long serialVersionUID = -4013227058179840505L;
    private static final Set<ProteomeType> REFERENCE_PROTEOME_TYPES =
            EnumSet.of(
                    ProteomeType.REFERENCE,
                    ProteomeType.REFERENCE_AND_REPRESENTATIVE,
                    ProteomeType.REPRESENTATIVE);

    @Override
    public Tuple2<String, TaxonomyStatisticsWrapper> call(ProteomeEntry entry) throws Exception {
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if (REFERENCE_PROTEOME_TYPES.contains(entry.getProteomeType())) {
            statisticsBuilder.referenceProteomeCount(1);
        }
        statisticsBuilder.proteomeCount(1);
        TaxonomyStatisticsWrapper statisticsWrapper =
                TaxonomyStatisticsWrapper.builder().statistics(statisticsBuilder.build()).build();
        String taxonomyId = String.valueOf(entry.getTaxonomy().getTaxonId());
        return new Tuple2<>(taxonomyId, statisticsWrapper);
    }
}

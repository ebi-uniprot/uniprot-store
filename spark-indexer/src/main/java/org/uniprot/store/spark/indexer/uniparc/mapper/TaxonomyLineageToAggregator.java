package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.taxonomy.TaxonomyLineage;

public class TaxonomyLineageToAggregator
        implements Function2<
                        List<List<TaxonomyLineage>>,
                        List<TaxonomyLineage>,
                        List<List<TaxonomyLineage>>>,
                Serializable {
    @Serial private static final long serialVersionUID = -2592908497908003222L;

    @Override
    public List<List<TaxonomyLineage>> call(
            List<List<TaxonomyLineage>> accumulator, List<TaxonomyLineage> value) throws Exception {
        accumulator.add(value);
        return accumulator;
    }
}

package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.taxonomy.TaxonomyLineage;

public class TaxonomyLineageAccumulatorsMerger
        implements Function2<
                        List<List<TaxonomyLineage>>,
                        List<List<TaxonomyLineage>>,
                        List<List<TaxonomyLineage>>>,
                Serializable {
    @Serial private static final long serialVersionUID = -4910101980110069689L;

    @Override
    public List<List<TaxonomyLineage>> call(
            List<List<TaxonomyLineage>> accumulators1, List<List<TaxonomyLineage>> accumulators2)
            throws Exception {
        accumulators1.addAll(accumulators2);
        return accumulators1;
    }
}

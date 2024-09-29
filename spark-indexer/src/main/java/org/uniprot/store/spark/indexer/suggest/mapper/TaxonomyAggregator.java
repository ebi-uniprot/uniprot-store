package org.uniprot.store.spark.indexer.suggest.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import java.io.Serial;

public class TaxonomyAggregator implements Function2<String, String, String> {
    @Serial
    private static final long serialVersionUID = 1044752852946622425L;

    @Override
    public String call(String tax1, String tax2) throws Exception {
        return SparkUtils.getNotNullEntry(tax1, tax2);
    }
}

package org.uniprot.store.spark.indexer.suggest.mapper;

import java.io.Serial;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

public class SuggestDocumentAggregator
        implements Function2<SuggestDocument, SuggestDocument, SuggestDocument> {
    @Serial private static final long serialVersionUID = 1044752852946622425L;

    @Override
    public SuggestDocument call(SuggestDocument doc1, SuggestDocument doc2) throws Exception {
        return SparkUtils.getNotNullEntry(doc1, doc2);
    }
}

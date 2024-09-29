package org.uniprot.store.spark.indexer.suggest.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * This class merge two SuggestDocuments into one. During the merge we choose the one with highest
 * importance and also merge all the alternatives names.
 *
 * @author lgonzales
 * @since 2020-01-21
 */
public class TaxonomyHighImportanceReduce
        implements Function2<SuggestDocument, SuggestDocument, SuggestDocument> {

    private static final long serialVersionUID = 7280298514026843313L;

    @Override
    public SuggestDocument call(SuggestDocument doc1, SuggestDocument doc2) throws Exception {
        SuggestDocument important = getMostImportant(doc1, doc2);
        if (!important.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
            Set<String> altValues = new HashSet<>();
            if (Utils.notNull(doc1) && Utils.notNullNotEmpty(doc1.altValues)) {
                altValues.addAll(doc1.altValues);
            }
            if (Utils.notNull(doc2) && Utils.notNullNotEmpty(doc2.altValues)) {
                altValues.addAll(doc2.altValues);
            }
            important.altValues = new ArrayList<>(altValues);
        }
        return important;
    }

    private SuggestDocument getMostImportant(SuggestDocument doc1, SuggestDocument doc2) {
        SuggestDocument result = doc1;
        if(SparkUtils.isThereAnyNullEntry(doc1,doc2)){
            result = SparkUtils.getNotNullEntry(doc1, doc2);
        } else {
            if (!doc2.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
                result = doc2;
                result.value = doc1.value;
            } else if (!doc1.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
                result.value = doc2.value;
            }
        }
        return result;
    }
}

package org.uniprot.store.spark.indexer.suggest.mapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
public class TaxonomyHighImportanceReduce
        implements Function2<SuggestDocument, SuggestDocument, SuggestDocument> {

    private static final long serialVersionUID = 7280298514026843313L;

    @Override
    public SuggestDocument call(SuggestDocument v1, SuggestDocument v2) throws Exception {
        SuggestDocument important = getMostImportant(v1, v2);
        if (!important.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
            Set<String> altValues = new HashSet<>();
            if (Utils.notNullOrEmpty(v1.altValues)) {
                altValues.addAll(v1.altValues);
            }
            if (Utils.notNullOrEmpty(v2.altValues)) {
                altValues.addAll(v2.altValues);
            }
            important.altValues = new ArrayList<>(altValues);
        }
        return important;
    }

    private SuggestDocument getMostImportant(SuggestDocument v1, SuggestDocument v2) {
        SuggestDocument result = v1;
        if (!v2.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
            result = v2;
            result.value = v1.value;
        } else if (!v1.importance.equals(SuggestDocument.DEFAULT_IMPORTANCE)) {
            result.value = v2.value;
        }
        return result;
    }
}

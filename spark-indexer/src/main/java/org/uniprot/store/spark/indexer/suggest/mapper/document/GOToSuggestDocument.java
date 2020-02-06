package org.uniprot.store.spark.indexer.suggest.mapper.document;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.go.relations.GOTerm;

import scala.Tuple2;

/**
 * This class converts a GOTerm and its ancestors to a Iterable of SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class GOToSuggestDocument
        implements Function<Tuple2<GOTerm, String>, Iterable<SuggestDocument>> {
    private static final long serialVersionUID = -1004520025444037939L;

    @Override
    public Iterable<SuggestDocument> call(Tuple2<GOTerm, String> tuple) throws Exception {
        List<SuggestDocument> result = new ArrayList<>();
        GOTerm goTerm = tuple._1;
        result.add(buildSuggestDocument(goTerm));
        if (Utils.notNullNotEmpty(goTerm.getAncestors())) {
            goTerm.getAncestors()
                    .forEach(
                            ancestor -> {
                                result.add(buildSuggestDocument(ancestor));
                            });
        }
        return result;
    }

    private SuggestDocument buildSuggestDocument(GOTerm goTerm) {
        return SuggestDocument.builder()
                .id(goTerm.getId())
                .value(goTerm.getName())
                .dictionary(SuggestDictionary.GO.name())
                .build();
    }
}

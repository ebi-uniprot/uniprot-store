package org.uniprot.store.spark.indexer.suggest.mapper.document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts a GeneOntologyEntry and its ancestors to a Iterable of SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class GOToSuggestDocument
        implements FlatMapFunction<Tuple2<GeneOntologyEntry, String>, SuggestDocument> {
    private static final long serialVersionUID = -1004520025444037939L;

    @Override
    public Iterator<SuggestDocument> call(Tuple2<GeneOntologyEntry, String> tuple)
            throws Exception {
        List<SuggestDocument> result = new ArrayList<>();
        GeneOntologyEntry goTerm = tuple._1;
        result.add(buildSuggestDocument(goTerm));
        if (Utils.notNullNotEmpty(goTerm.getAncestors())) {
            goTerm.getAncestors().forEach(ancestor -> result.add(buildSuggestDocument(ancestor)));
        }
        return result.iterator();
    }

    private SuggestDocument buildSuggestDocument(GeneOntologyEntry goTerm) {
        String idOnly = goTerm.getId().substring(3); // remove "GO:" prefix
        return SuggestDocument.builder()
                .id(idOnly)
                .value(goTerm.getName())
                .altValue(goTerm.getId())
                .dictionary(SuggestDictionary.GO.name())
                .build();
    }
}

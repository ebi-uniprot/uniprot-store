package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.uniprot.core.util.Utils.nullOrEmpty;
import static org.uniprot.store.spark.indexer.uniprot.mapper.ChebiToUniProtDocument.CHEBI_PREFIX;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts a ChebiEntry entry to a SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiToSuggestDocument
        implements PairFlatMapFunction<
                Tuple2<String, Tuple2<String, ChebiEntry>>, String, SuggestDocument> {
    private static final long serialVersionUID = 1671999070788047349L;
    private final String dictionaryName;

    public ChebiToSuggestDocument(String dictionaryName) {
        this.dictionaryName = dictionaryName;
    }

    @Override
    public Iterator<Tuple2<String, SuggestDocument>> call(
            Tuple2<String, Tuple2<String, ChebiEntry>> tuple) throws Exception {
        List<Tuple2<String, SuggestDocument>> result = new ArrayList<>();
        ChebiEntry chebi = tuple._2._2;
        result.add(convertEntry(chebi));
        if (Utils.notNullNotEmpty(chebi.getRelatedIds())) {
            chebi.getRelatedIds().stream().map(this::convertEntry).forEach(result::add);
        }
        return result.iterator();
    }

    private Tuple2<String, SuggestDocument> convertEntry(ChebiEntry entry) {
        String chebiId = entry.getId();
        if (!chebiId.startsWith(CHEBI_PREFIX)) {
            chebiId = CHEBI_PREFIX + chebiId;
        }
        SuggestDocument.SuggestDocumentBuilder suggestionBuilder =
                SuggestDocument.builder()
                        .id(chebiId)
                        .dictionary(dictionaryName)
                        .value(entry.getName());
        if (!nullOrEmpty(entry.getSynonyms())) {
            suggestionBuilder.altValues(entry.getSynonyms());
        }
        if (!nullOrEmpty(entry.getInchiKey())) {
            suggestionBuilder.altValue(entry.getInchiKey());
        }
        return new Tuple2<>(entry.getId(), suggestionBuilder.build());
    }
}

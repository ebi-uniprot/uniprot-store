package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.uniprot.core.util.Utils.nullOrEmpty;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts a ChebiEntry entry to a SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class ChebiToSuggestDocument
        implements Function<Tuple2<String, ChebiEntry>, SuggestDocument> {
    private static final long serialVersionUID = 1671999070788047349L;
    private final String dictionaryName;

    public ChebiToSuggestDocument(String dictionaryName) {
        this.dictionaryName = dictionaryName;
    }

    @Override
    public SuggestDocument call(Tuple2<String, ChebiEntry> tuple) throws Exception {
        ChebiEntry chebi = tuple._2;
        SuggestDocument.SuggestDocumentBuilder suggestionBuilder =
                SuggestDocument.builder()
                        .id(tuple._1)
                        .dictionary(dictionaryName)
                        .value(chebi.getName());
        if (!nullOrEmpty(chebi.getInchiKey())) {
            suggestionBuilder.altValue(chebi.getInchiKey());
        }
        return suggestionBuilder.build();
    }
}

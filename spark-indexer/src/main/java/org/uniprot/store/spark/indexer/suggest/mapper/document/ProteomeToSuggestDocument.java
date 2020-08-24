package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeToSuggestDocument implements Function<ProteomeEntry, SuggestDocument> {
    @Override
    public SuggestDocument call(ProteomeEntry entry) throws Exception {
        SuggestDocument.SuggestDocumentBuilder builder =
                SuggestDocument.builder()
                        .id(entry.getId().getValue())
                        .value(entry.getDescription()) // it is name actually since there is no
                        // field for name in ProteomeEntry
                        .altValue(entry.getId().getValue())
                        .dictionary(SuggestDictionary.PROTEOME_UPID.name());
        return builder.build();
    }
}

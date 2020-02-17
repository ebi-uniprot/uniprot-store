package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts a ECEntry entry to a SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-17
 */
public class ECToSuggestDocument implements Function<Tuple2<String, ECEntry>, SuggestDocument> {
    private static final long serialVersionUID = -1004520025444037939L;

    @Override
    public SuggestDocument call(Tuple2<String, ECEntry> tuple) throws Exception {
        ECEntry ec = tuple._2;
        return SuggestDocument.builder()
                .id(ec.id())
                .value(ec.label())
                .dictionary(SuggestDictionary.EC.name())
                .build();
    }
}

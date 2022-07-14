package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.uniprot.store.search.field.SuggestField.Importance.low;

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
        var builder =
                SuggestDocument.builder()
                        .id(ec.getId())
                        .value(ec.getLabel())
                        .dictionary(SuggestDictionary.EC.name());
        if (ec.getLabel().startsWith("Transferred")) {
            builder.importance(low.name());
        }
        return builder.build();
    }
}

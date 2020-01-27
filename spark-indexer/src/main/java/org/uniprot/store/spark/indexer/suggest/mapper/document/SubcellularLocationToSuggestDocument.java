package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-16
 */
public class SubcellularLocationToSuggestDocument
        implements Function<Tuple2<String, SubcellularLocationEntry>, SuggestDocument> {
    private static final long serialVersionUID = -6696340511039452597L;

    @Override
    public SuggestDocument call(Tuple2<String, SubcellularLocationEntry> tuple) throws Exception {
        SubcellularLocationEntry subcell = tuple._2;
        return SuggestDocument.builder()
                .id(subcell.getAccession())
                .value(subcell.getId())
                .dictionary(SuggestDictionary.SUBCELL.name())
                .build();
    }
}

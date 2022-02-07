package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

import scala.Tuple2;

public class RheaCompToSuggestDocument
        implements Function<Tuple2<String, RheaComp>, SuggestDocument> {
    private static final long serialVersionUID = 8357821528032045219L;

    @Override
    public SuggestDocument call(Tuple2<String, RheaComp> tuple) throws Exception {
        RheaComp rheaComp = tuple._2;
        return SuggestDocument.builder()
                .id(rheaComp.getId())
                .value(rheaComp.getName())
                .dictionary(SuggestDictionary.CATALYTIC_ACTIVITY.name())
                .build();
    }
}

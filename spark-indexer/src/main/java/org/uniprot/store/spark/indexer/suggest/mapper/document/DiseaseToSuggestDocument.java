package org.uniprot.store.spark.indexer.suggest.mapper.document;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

public class DiseaseToSuggestDocument implements Function<DiseaseEntry, SuggestDocument> {

    private static final long serialVersionUID = -9153253079695386051L;

    @Override
    public SuggestDocument call(DiseaseEntry diseaseEntry) throws Exception {
        return SuggestDocument.builder()
                .id(diseaseEntry.getId())
                .value(diseaseEntry.getName())
                .altValues(extractAltValues(diseaseEntry))
                .dictionary(SuggestDictionary.DISEASE.name())
                .build();
    }

    private List<String> extractAltValues(DiseaseEntry diseaseEntry) {
        List<String> altValues = new ArrayList<>();
        List<String> altNames = diseaseEntry.getAlternativeNames();
        if (Utils.notNullNotEmpty(altNames)) {
            altValues.addAll(altNames);
        }
        if (Utils.notNullNotEmpty(diseaseEntry.getAcronym())) {
            altValues.add(diseaseEntry.getAcronym());
        }
        return altValues;
    }
}

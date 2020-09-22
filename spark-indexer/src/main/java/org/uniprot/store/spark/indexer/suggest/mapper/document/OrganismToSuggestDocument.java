package org.uniprot.store.spark.indexer.suggest.mapper.document;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts an Organism entry to a SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class OrganismToSuggestDocument
        implements Function<Tuple2<String, List<TaxonomyLineage>>, SuggestDocument> {

    private static final long serialVersionUID = -1165900093496594503L;
    private final String dictionaryName;

    public OrganismToSuggestDocument(String dictionaryName) {
        this.dictionaryName = dictionaryName;
    }

    @Override
    public SuggestDocument call(Tuple2<String, List<TaxonomyLineage>> tuple) throws Exception {
        TaxonomyLineage organism = tuple._2.get(0);
        return getOrganismSuggestDocument(organism, dictionaryName);
    }

    static SuggestDocument getOrganismSuggestDocument(
            TaxonomyLineage organism, String dictionaryName) {

        SuggestDocument.SuggestDocumentBuilder builder =
                SuggestDocument.builder()
                        .id(String.valueOf(organism.getTaxonId()))
                        .altValues(extractAltValuesFromOrganism(organism))
                        .dictionary(dictionaryName);

        if (Utils.notNullNotEmpty(organism.getScientificName())) {
            builder.value(organism.getScientificName());
        }

        return builder.build();
    }

    private static List<String> extractAltValuesFromOrganism(TaxonomyLineage organism) {
        List<String> altValues = new ArrayList<>();
        if (Utils.notNullNotEmpty(organism.getCommonName())) {
            altValues.add(organism.getCommonName());
        }
        if (Utils.notNullNotEmpty(organism.getSynonyms())) {
            altValues.addAll(organism.getSynonyms());
        }
        return altValues;
    }
}

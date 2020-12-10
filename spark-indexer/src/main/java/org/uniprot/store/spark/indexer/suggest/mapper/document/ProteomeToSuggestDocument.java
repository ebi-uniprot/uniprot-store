package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 21/08/2020
 */
public class ProteomeToSuggestDocument
        implements Function<Tuple2<String, TaxonomyEntry>, SuggestDocument> {

    private static final long serialVersionUID = -8220425041832583808L;

    @Override
    public SuggestDocument call(Tuple2<String, TaxonomyEntry> tuple) throws Exception {
        String proteomeId = tuple._1;
        // value is taxonomy scientificName since there is no name in Proteome Xml file
        String value = tuple._2.getScientificName();
        SuggestDocument.SuggestDocumentBuilder builder =
                SuggestDocument.builder()
                        .id(proteomeId)
                        .value(value)
                        .altValue(proteomeId)
                        .dictionary(SuggestDictionary.PROTEOME_UPID.name());
        return builder.build();
    }
}

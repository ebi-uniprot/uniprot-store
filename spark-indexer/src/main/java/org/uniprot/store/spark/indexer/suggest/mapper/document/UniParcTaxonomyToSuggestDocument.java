package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.uniprot.store.search.document.suggest.SuggestDictionary.UNIPARC_TAXONOMY;
import static org.uniprot.store.spark.indexer.suggest.mapper.document.OrganismToSuggestDocument.getOrganismSuggestDocument;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 25/08/2020
 */
public class UniParcTaxonomyToSuggestDocument
        implements PairFlatMapFunction<
                Tuple2<String, Tuple2<String, Optional<List<TaxonomyLineage>>>>,
                String,
                SuggestDocument> {

    @Override
    public Iterator<Tuple2<String, SuggestDocument>> call(
            Tuple2<String, Tuple2<String, Optional<List<TaxonomyLineage>>>> tuple)
            throws Exception {
        String dictionary = UNIPARC_TAXONOMY.name();

        return tuple._2._2
                .or(
                        Collections.singletonList(
                                new TaxonomyLineageBuilder()
                                        .taxonId(Long.parseLong(tuple._1))
                                        .build()))
                .stream()
                .map(taxon -> getOrganismSuggestDocument(taxon, dictionary))
                .map(suggestDocument -> new Tuple2<>(suggestDocument.id, suggestDocument))
                .iterator();
    }
}

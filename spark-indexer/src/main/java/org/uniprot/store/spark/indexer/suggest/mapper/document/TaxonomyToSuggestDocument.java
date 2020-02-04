package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.uniprot.store.spark.indexer.suggest.mapper.document.OrganismToSuggestDocument.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * This class converts an Organism and its Lineage to an Iterator of SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class TaxonomyToSuggestDocument
        implements PairFlatMapFunction<
                Tuple2<String, Tuple2<String, List<TaxonomyLineage>>>, String, SuggestDocument> {
    private static final long serialVersionUID = -8627404051025050159L;

    @Override
    public Iterator<Tuple2<String, SuggestDocument>> call(
            Tuple2<String, Tuple2<String, List<TaxonomyLineage>>> tuple) throws Exception {
        List<Tuple2<String, SuggestDocument>> result = new ArrayList<>();
        List<TaxonomyLineage> organisms = tuple._2._2;
        String dictionary = SuggestDictionary.TAXONOMY.name();
        organisms.forEach(
                organism -> {
                    SuggestDocument suggestDocument =
                            getOrganismSuggestDocument(organism, dictionary);
                    result.add(new Tuple2<>(suggestDocument.id, suggestDocument));
                });
        return result.iterator();
    }
}

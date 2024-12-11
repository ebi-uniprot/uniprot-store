package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.*;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;

import scala.Tuple2;

public class UniParcTaxonomySequenceSourceJoin
        implements Function<
                Tuple2<Optional<Iterable<TaxonomyEntry>>, Optional<Map<String, Set<String>>>>,
                UniParcTaxonomySequenceSource> {
    @Serial private static final long serialVersionUID = -8107763749949319147L;

    @Override
    public UniParcTaxonomySequenceSource call(
            Tuple2<Optional<Iterable<TaxonomyEntry>>, Optional<Map<String, Set<String>>>> tuple2)
            throws Exception {
        Iterable<TaxonomyEntry> taxonomyEntries = new ArrayList<>();
        if (tuple2._1.isPresent()) {
            taxonomyEntries = tuple2._1.get();
        }
        return new UniParcTaxonomySequenceSource(
                taxonomyEntries, tuple2._2.orElse(new HashMap<>()));
    }
}

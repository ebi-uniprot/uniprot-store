package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;

import scala.Tuple2;

public class TaxonomyHostsJoinMapper
        implements Function<Tuple2<TaxonomyEntry, Optional<Iterable<Taxonomy>>>, TaxonomyEntry> {

    private static final long serialVersionUID = -8899670436776103979L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, Optional<Iterable<Taxonomy>>> tuple)
            throws Exception {
        TaxonomyEntry entry = tuple._1;
        if (tuple._2.isPresent()) {
            TaxonomyEntryBuilder builder = TaxonomyEntryBuilder.from(entry);
            for (Taxonomy host : tuple._2.get()) {
                builder.hostsAdd(host);
            }
            entry = builder.build();
        }
        return entry;
    }
}

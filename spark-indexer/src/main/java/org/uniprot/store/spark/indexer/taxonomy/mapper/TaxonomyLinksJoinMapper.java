package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;

import scala.Tuple2;

public class TaxonomyLinksJoinMapper
        implements Function<
                Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>>, TaxonomyEntry> {

    private static final long serialVersionUID = -7413788674602610060L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, Optional<Iterable<TaxonomyEntry>>> tuple)
            throws Exception {
        TaxonomyEntry result = tuple._1;
        if (tuple._2.isPresent()) {
            TaxonomyEntryBuilder builder = TaxonomyEntryBuilder.from(result);
            for (TaxonomyEntry linkEntry : tuple._2.get()) {
                builder.linksAdd(linkEntry.getLinks().get(0));
            }
            result = builder.build();
        }
        return result;
    }
}

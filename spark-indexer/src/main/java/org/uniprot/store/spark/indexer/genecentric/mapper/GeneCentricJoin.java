package org.uniprot.store.spark.indexer.genecentric.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 22/10/2020
 */
public class GeneCentricJoin
        implements Function<
                Tuple2<GeneCentricEntry, Optional<Iterable<GeneCentricEntry>>>, GeneCentricEntry> {
    private static final long serialVersionUID = 3717986765367286622L;

    @Override
    public GeneCentricEntry call(
            Tuple2<GeneCentricEntry, Optional<Iterable<GeneCentricEntry>>> tuple) throws Exception {
        GeneCentricEntryBuilder canonicalBuilder = GeneCentricEntryBuilder.from(tuple._1);
        if (tuple._2.isPresent()) {
            Iterable<GeneCentricEntry> relatedList = tuple._2.get();
            relatedList.forEach(
                    related -> {
                        if (Utils.notNull(related.getRelatedProteins())) {
                            related.getRelatedProteins()
                                    .forEach(canonicalBuilder::relatedProteinsAdd);
                        }
                    });
        }
        return canonicalBuilder.build();
    }
}

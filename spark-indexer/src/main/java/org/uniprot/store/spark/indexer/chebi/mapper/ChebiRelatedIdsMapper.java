package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/** Returns and RDD with Tuple2<relatedId, chebiId> */
public class ChebiRelatedIdsMapper implements PairFlatMapFunction<ChebiEntry, Long, Long> {
    private static final long serialVersionUID = 1456428770660918572L;

    /**
     * This mapper extracts all relatedIds from each ChebiEntry
     *
     * @param entry ChebiEntry entry
     * @return Tuple2<relatedId, chebiId>
     * @throws Exception
     */
    @Override
    public Iterator<Tuple2<Long, Long>> call(ChebiEntry entry) throws Exception {
        final Long entryId = Long.parseLong(entry.getId());
        if (Utils.notNullNotEmpty(entry.getRelatedIds())) {
            return entry.getRelatedIds().stream()
                    .map(ChebiEntry::getId)
                    .map(Long::parseLong)
                    .map(relatedId -> new Tuple2<>(relatedId, entryId))
                    .collect(Collectors.toList())
                    .iterator();
        } else {
            return Collections.emptyIterator();
        }
    }
}

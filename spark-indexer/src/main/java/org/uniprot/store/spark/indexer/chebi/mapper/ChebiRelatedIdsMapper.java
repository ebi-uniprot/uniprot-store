package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<Tuple2<Long, Long>> result = new ArrayList<>();
        if (Utils.notNullNotEmpty(entry.getRelatedIds())) {
            result.addAll(mapRelatedIds(entry, entryId));
        }

        if (Utils.notNullNotEmpty(entry.getMajorMicrospecies())) {
            result.addAll(mapMajorMicrospecies(entry, entryId));
        }
        return result.iterator();
    }

    private List<Tuple2<Long, Long>> mapRelatedIds(ChebiEntry entry, Long entryId) {
        return entry.getRelatedIds().stream()
                .map(ChebiEntry::getId)
                .map(Long::parseLong)
                .map(relatedId -> new Tuple2<>(relatedId, entryId))
                .collect(Collectors.toList());
    }

    private List<Tuple2<Long, Long>> mapMajorMicrospecies(ChebiEntry entry, Long entryId) {
        return entry.getMajorMicrospecies().stream()
                .map(ChebiEntry::getId)
                .map(Long::parseLong)
                .flatMap(majorMicrospecieId -> createMajorMicroespeciesRelation(entryId, majorMicrospecieId))
                .collect(Collectors.toList());
    }

    /**
     * This method creates a bi-directional relation for majorMicrospecies relations
     * @param entryId ChebiId
     * @param majorMicrospecieId Related majorMicrospecie ChebiId
     * @return bi-directional majorMicrospecies relations
     */
    private Stream<Tuple2<Long, Long>> createMajorMicroespeciesRelation(Long  entryId, Long majorMicrospecieId) {
        Tuple2<Long, Long> relation1 = new Tuple2<>(entryId, majorMicrospecieId);
        Tuple2<Long, Long> relation2 = new Tuple2<>(majorMicrospecieId, entryId);
        return Stream.of(relation1, relation2);
    }
}

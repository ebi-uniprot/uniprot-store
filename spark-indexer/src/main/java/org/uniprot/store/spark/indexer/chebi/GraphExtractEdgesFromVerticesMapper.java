package org.uniprot.store.spark.indexer.chebi;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.graphx.Edge;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

/**
 * GraphExtractEdgesFromVerticesMapper is a Flat Map class that create all Edges among a Chebi entry
 * and all its related Ids.
 */
class GraphExtractEdgesFromVerticesMapper
        implements FlatMapFunction<Tuple2<Object, ChebiEntry>, Edge<String>> {

    private static final long serialVersionUID = -8581725028718247525L;

    @Override
    public java.util.Iterator<Edge<String>> call(Tuple2<Object, ChebiEntry> tuple)
            throws Exception {
        try {
            List<Edge<String>> result = new ArrayList<>();
            if (tuple._2 != null) {
                ChebiEntry entry = tuple._2;
                long fromId = Long.parseLong(entry.getId());
                for (ChebiEntry related : entry.getRelatedIds()) {
                    result.add(new Edge<>(fromId, Long.parseLong(related.getId()), "isA"));
                }
            }
            return result.iterator();
        } catch (Exception e) {
            throw new SparkIndexException("Error loading Edge: " + tuple._1, e);
        }
    }
}

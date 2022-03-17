package org.uniprot.store.spark.indexer.chebi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.graphx.EdgeTriplet;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;

/**
 * GraphSendVertexMessageMapper is a flat map that expand the graph to the next Chebi relatedIds
 * (graph leafs)
 */
class GraphSendVertexMessageMapper
        implements Function1<EdgeTriplet<ChebiEntry, String>, Iterator<Tuple2<Object, ChebiEntry>>>,
                Serializable {

    private static final long serialVersionUID = -5599613474605702249L;

    @Override
    public Iterator<Tuple2<Object, ChebiEntry>> apply(EdgeTriplet<ChebiEntry, String> triplet) {
        List<Tuple2<Object, ChebiEntry>> result = new ArrayList<>();
        Object id = triplet.srcId();
        ChebiEntryBuilder builder = ChebiEntryBuilder.from(triplet.srcAttr());
        if (triplet.dstAttr() != null) {
            for (ChebiEntry related : triplet.dstAttr().getRelatedIds()) {
                if (!triplet.srcAttr().getId().equals(related.getId())) {
                    builder.relatedIdsAdd(related);
                }
            }
        }
        ChebiEntry src = builder.build();
        result.add(new Tuple2<>(id, src));

        return JavaConverters.asScalaIteratorConverter(result.iterator()).asScala();
    }

    /*
        @Override
    public Iterator<Tuple2<Object, ChebiEntry>> apply(EdgeTriplet<ChebiEntry, String> triplet) {
        List<Tuple2<Object, ChebiEntry>> result = new ArrayList<>();
        ChebiEntryBuilder srcBuilder = ChebiEntryBuilder.from(triplet.srcAttr());
        if (triplet.dstAttr() != null) {
            for (ChebiEntry dstRelated : triplet.dstAttr().getRelatedIds()) {
                if (!triplet.srcAttr().getId().equals(dstRelated.getId())) {
                    srcBuilder.relatedIdsAdd(dstRelated);
                }
            }

            ChebiEntryBuilder dstBuilder = ChebiEntryBuilder.from(triplet.dstAttr());
            for (ChebiEntry srcRelated : triplet.srcAttr().getRelatedIds()) {
                if (!triplet.dstAttr().getId().equals(srcRelated.getId())) {
                    dstBuilder.relatedIdsAdd(srcRelated);
                }
            }
            ChebiEntry dst = dstBuilder.build();
            Object dstId = triplet.dstId();
            result.add(new Tuple2<>(dstId, dst));
        }
        ChebiEntry src = srcBuilder.build();
        Object srcId = triplet.srcId();
        result.add(new Tuple2<>(srcId, src));

        return JavaConverters.asScalaIteratorConverter(result.iterator()).asScala();
    }
     */
}

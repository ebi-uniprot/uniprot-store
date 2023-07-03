package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class UniProtIdTrackerJoinMapper
        implements Function<Tuple2<UniProtDocument, Optional<Set<String>>>, UniProtDocument> {
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Set<String>>> tuple2)
            throws Exception {
        UniProtDocument doc = tuple2._1;
        if (tuple2._2.isPresent()) {
            if (!doc.reviewed) {
                throw new SparkIndexException(
                        "Only Swiss-Prot entries are expected in UniProtIdTrackerJoinMapper");
            }
            Set<String> trackedIds = tuple2._2.get();
            doc.id.addAll(trackedIds);
            doc.idDefault.addAll(trackedIds);
        }
        return doc;
    }
}

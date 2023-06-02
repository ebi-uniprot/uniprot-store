package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

public class UniProtIdTrackerJoinMapper
        implements Function<Tuple2<UniProtDocument, Optional<Set<String>>>, UniProtDocument> {
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Set<String>>> tuple2)
            throws Exception {
        UniProtDocument doc = tuple2._1;
        if (tuple2._2.isPresent()) {
            Set<String> trackedIds = tuple2._2.get();
            doc.id.addAll(trackedIds);
            if (doc.reviewed) {
                // first component of swiss-prot id is gene, which we want searchable in the id
                doc.idDefault.addAll(trackedIds);
            } else {
                // don't add first component for trembl entries, since this is the accession, and
                // we do not want false boosting for default searches that match a substring of
                // the accession
                Set<String> idsWithoutAccession =
                        trackedIds.stream()
                                .map(id -> id.split("_"))
                                .filter(ids -> ids.length == 2)
                                .map(ids -> ids[1])
                                .collect(Collectors.toSet());
                doc.idDefault.addAll(idsWithoutAccession);
                doc.content.addAll(trackedIds);
            }
        }
        return doc;
    }
}

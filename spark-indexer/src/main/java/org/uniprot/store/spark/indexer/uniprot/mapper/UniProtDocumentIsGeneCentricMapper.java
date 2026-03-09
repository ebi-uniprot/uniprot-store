package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

public class UniProtDocumentIsGeneCentricMapper
        implements Function<Tuple2<UniProtDocument, Optional<Boolean>>, UniProtDocument> {
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Boolean>> tuple2)
            throws Exception {
        UniProtDocument document = tuple2._1();
        document.isGeneCentric = tuple2._2.orElse(false);
        return document;
    }
}

package org.uniprot.store.spark.indexer.precomputed.mapper;

import java.io.Serial;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

import scala.Tuple2;

public class PrecomputedAnnotationDocumentProteomeJoin
        implements Function<
                Tuple2<PrecomputedAnnotationDocument, Optional<Iterable<String>>>,
                PrecomputedAnnotationDocument> {

    @Serial private static final long serialVersionUID = 7884956379509205748L;

    @Override
    public PrecomputedAnnotationDocument call(
            Tuple2<PrecomputedAnnotationDocument, Optional<Iterable<String>>> entryProteomeIds) {
        PrecomputedAnnotationDocument document = entryProteomeIds._1;
        return document.toBuilder()
                .proteome(
                        StreamSupport.stream(
                                        entryProteomeIds._2.orElse(List.of()).spliterator(), false)
                                .distinct()
                                .toList())
                .build();
    }
}

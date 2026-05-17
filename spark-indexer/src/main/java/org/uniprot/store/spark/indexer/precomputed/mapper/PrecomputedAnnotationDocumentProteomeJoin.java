package org.uniprot.store.spark.indexer.precomputed.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class PrecomputedAnnotationDocumentProteomeJoin
        implements Function<
                Tuple2<PrecomputedAnnotationDocument, Optional<Iterable<String>>>,
                PrecomputedAnnotationDocument> {

    private static final long serialVersionUID = 7884956379509205748L;

    @Override
    public PrecomputedAnnotationDocument call(
            Tuple2<PrecomputedAnnotationDocument, Optional<Iterable<String>>> entryProteomeIds) {
        PrecomputedAnnotationDocument document = entryProteomeIds._1;
        if (!entryProteomeIds._2.isPresent()) {
            throw new SparkIndexException(
                    "Unable to find proteome ids for precomputed annotation accession: "
                            + document.getAccession());
        }
        return PrecomputedAnnotationEntryToDocument.withProteomes(
                document, entryProteomeIds._2.get());
    }
}

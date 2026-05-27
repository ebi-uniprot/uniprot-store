package org.uniprot.store.spark.indexer.precomputed.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

import scala.Tuple2;

public class TaxIdToPrecomputedAnnotationDocument
        implements Serializable,
                PairFunction<
                        PrecomputedAnnotationDocument, Integer, PrecomputedAnnotationDocument> {
    @Override
    public Tuple2<Integer, PrecomputedAnnotationDocument> call(PrecomputedAnnotationDocument doc)
            throws Exception {
        return new Tuple2<>(doc.getTaxonomyId(), doc);
    }
}

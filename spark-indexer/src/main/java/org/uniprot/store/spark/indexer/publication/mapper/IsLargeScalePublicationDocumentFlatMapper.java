package org.uniprot.store.spark.indexer.publication.mapper;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.store.search.document.publication.PublicationDocument;

import scala.Tuple2;

/**
 * Converts a {@link Tuple2} of <count, Iterable<PublicationDocument.Builder> into an iterator of
 * {@link PublicationDocument.Builder}.
 *
 * <p>Tuple2._1 => the publication ID. If < 0, this implies the publication has no pubmed ID. If
 * Tuple2._1 > 0, it represents the pubmed ID. We only set {@link
 * PublicationDocument#isLargeScale()} for publications with a pubmed ID.
 *
 * <p>Tuple2._2 => all publications with the same pubmed ID. Therefore, to compute {@link
 * PublicationDocument#isLargeScale()}, we check the size of Tuple2._2. If it is greater than 50,
 * then it is classed as large scale.
 *
 * <p>Created 21/01/2021
 *
 * @author Edd
 */
public class IsLargeScalePublicationDocumentFlatMapper
        implements FlatMapFunction<
                Tuple2<String, Iterable<PublicationDocument.Builder>>,
                PublicationDocument.Builder> {
    private static final long serialVersionUID = 8799695653013196890L;

    @Override
    public Iterator<PublicationDocument.Builder> call(
            Tuple2<String, Iterable<PublicationDocument.Builder>> tuple) throws Exception {
        boolean isLargeScale = isLargeScale(tuple._2);
        tuple._2.forEach(builder -> builder.isLargeScale(isLargeScale));

        return tuple._2.iterator();
    }

    private boolean isLargeScale(Iterable<PublicationDocument.Builder> docsWithSamePubMed) {
        return StreamSupport.stream(docsWithSamePubMed.spliterator(), false).count() > 50;
    }
}

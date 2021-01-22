package org.uniprot.store.spark.indexer.publication.mapper;

import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.store.search.document.publication.PublicationDocument;

import scala.Tuple2;

/**
 * Created 21/01/2021
 *
 * @author Edd
 */
public class IsLargeScalePublicationDocumentFlatMapper
        implements FlatMapFunction<
                Tuple2<Integer, Iterable<PublicationDocument.Builder>>,
                PublicationDocument.Builder> {
    @Override
    public Iterator<PublicationDocument.Builder> call(
            Tuple2<Integer, Iterable<PublicationDocument.Builder>> tuple) throws Exception {
        if (tuple._1 >= 0) {
            boolean isLargeScale = isLargeScale(tuple._2);
            tuple._2.forEach(builder -> builder.isLargeScale(isLargeScale));
        }

        return tuple._2.iterator();
    }

    private boolean isLargeScale(Iterable<PublicationDocument.Builder> docsWithSamePubMed) {
        return StreamSupport.stream(docsWithSamePubMed.spliterator(), false).count() > 50;
    }
}

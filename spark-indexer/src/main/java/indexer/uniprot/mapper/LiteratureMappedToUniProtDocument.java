package indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedToUniProtDocument
        implements Function<
                Tuple2<UniProtDocument, Optional<Iterable<LiteratureMappedReference>>>,
                UniProtDocument> {
    private static final long serialVersionUID = -5057000958468900711L;

    @Override
    public UniProtDocument call(
            Tuple2<UniProtDocument, Optional<Iterable<LiteratureMappedReference>>> tuple)
            throws Exception {
        UniProtDocument doc = tuple._1;
        if (tuple._2.isPresent()) {
            tuple._2
                    .get()
                    .forEach(
                            mappedLiterature -> {
                                if (mappedLiterature.hasSourceId()) {
                                    doc.mappedCitation.add(mappedLiterature.getSourceId());
                                }
                            });
        }
        return doc;
    }
}

package indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * This class Merge a list of Mapped PubmedIds into UniProtDocument.mappedCitation
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedToUniProtDocument
        implements Function<Tuple2<UniProtDocument, Optional<Iterable<String>>>, UniProtDocument> {
    private static final long serialVersionUID = -5057000958468900711L;

    /**
     * @param tuple of UniProtDocument and an iterable of Mapped PubmedIds
     * @return UniProtDocument with all mappedCitation
     */
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<String>>> tuple)
            throws Exception {
        UniProtDocument doc = tuple._1;
        if (tuple._2.isPresent()) {
            tuple._2.get().forEach(doc.mappedCitation::add);
        }
        return doc;
    }
}

package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * This class Merge a list of Community and Computational PubmedIds into
 * UniProtDocument.communityPubmedIds and UniProtDocument.computationalPubmedIds
 *
 * @author lgonzales
 * @since 2019-12-02
 */
public class LiteratureMappedToUniProtDocument
        implements Function<
                Tuple2<UniProtDocument, Optional<Iterable<Tuple2<String, String>>>>,
                UniProtDocument> {
    private static final long serialVersionUID = -5057000958468900711L;

    /**
     * @param tuple of UniProtDocument and an iterable of community and computational PubmedIds
     * @return UniProtDocument with all communityPubmedIds and computationalPubmedIds
     */
    @Override
    public UniProtDocument call(
            Tuple2<UniProtDocument, Optional<Iterable<Tuple2<String, String>>>> tuple)
            throws Exception {
        UniProtDocument doc = tuple._1;
        if (tuple._2.isPresent()) {
            for (Tuple2<String, String> srcPubMedIds : tuple._2.get()) {
                if ("ORCID".equalsIgnoreCase(srcPubMedIds._1)) {
                    doc.communityPubmedIds.add(srcPubMedIds._2);
                } else {
                    doc.computationalPubmedIds.add(srcPubMedIds._2);
                }
            }
        }
        return doc;
    }
}

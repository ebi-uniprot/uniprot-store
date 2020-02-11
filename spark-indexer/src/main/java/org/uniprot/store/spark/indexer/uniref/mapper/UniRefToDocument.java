package org.uniprot.store.spark.indexer.uniref.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.spark.indexer.uniref.converter.UniRefDocumentConverter;

import scala.Tuple2;

/**
 * This class get an UniRefEntry and convert to UniRefDocument and returns a Tuple{key=taxId,
 * value=UniRefToDocument} so this tuple can be used to Join with Taxonomy data
 *
 * @author lgonzales
 * @since 2020-02-07
 */
public class UniRefToDocument
        implements Serializable, PairFunction<UniRefEntry, String, UniRefDocument> {
    private static final long serialVersionUID = -8683726483569457630L;

    @Override
    public Tuple2<String, UniRefDocument> call(UniRefEntry entry) throws Exception {
        UniRefDocumentConverter converter = new UniRefDocumentConverter();
        UniRefDocument document = converter.convert(entry);
        String taxId = String.valueOf(entry.getRepresentativeMember().getOrganismTaxId());
        return new Tuple2<>(taxId, document);
    }
}

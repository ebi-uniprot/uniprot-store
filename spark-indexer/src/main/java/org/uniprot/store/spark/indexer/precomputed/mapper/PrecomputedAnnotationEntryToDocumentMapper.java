package org.uniprot.store.spark.indexer.precomputed.mapper;

import java.io.Serial;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

public class PrecomputedAnnotationEntryToDocumentMapper
        implements Function<UniProtKBEntry, PrecomputedAnnotationDocument> {

    @Serial private static final long serialVersionUID = -6570815106805565054L;

    @Override
    public PrecomputedAnnotationDocument call(UniProtKBEntry uniProtKBEntry) throws Exception {
        String accession = uniProtKBEntry.getPrimaryAccession().getValue();
        String[] split = accession.split("-");
        return PrecomputedAnnotationDocument.builder()
                .accession(accession)
                .uniparc(split[0].trim())
                .taxonomyId(Integer.valueOf(split[1].trim()))
                .build();
    }
}

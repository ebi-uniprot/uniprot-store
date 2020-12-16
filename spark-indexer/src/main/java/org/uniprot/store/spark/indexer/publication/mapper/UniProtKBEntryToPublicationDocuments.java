package org.uniprot.store.spark.indexer.publication.mapper;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryReferencesConverter;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 15/12/2020
 */
public class UniProtKBEntryToPublicationDocuments
        implements FlatMapFunction<UniProtKBEntry, PublicationDocument> {
    private UniProtEntryReferencesConverter converter;

    public UniProtKBEntryToPublicationDocuments() {
        this.converter = new UniProtEntryReferencesConverter();
    }

    @Override
    public Iterator<PublicationDocument> call(UniProtKBEntry uniProtKBEntry) throws Exception {
        return this.converter.convertToPublicationDocuments(uniProtKBEntry).iterator();
    }
}

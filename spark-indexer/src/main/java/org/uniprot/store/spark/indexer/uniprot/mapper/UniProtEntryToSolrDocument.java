package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.uniprot.converter.InactiveUniprotEntryConverter;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverter;

/**
 * This class is responsible to Map UniProtkbEntry object to UniProtDocument
 *
 * @author lgonzales
 * @since 2019-11-12
 */
public class UniProtEntryToSolrDocument
        implements Serializable, Function<UniProtkbEntry, UniProtDocument> {

    private static final long serialVersionUID = -6891371730036443245L;
    private final Map<String, String> pathway;

    public UniProtEntryToSolrDocument(Map<String, String> pathway) {
        this.pathway = pathway;
    }

    /**
     * @param uniProtkbEntry extract UniProtkbEntry.
     * @return UniProtDocument with all information extracted from UniProtkbEntry.
     */
    @Override
    public UniProtDocument call(UniProtkbEntry uniProtkbEntry) throws Exception {
        UniProtDocument result;
        if (uniProtkbEntry.isActive()) {
            UniProtEntryConverter converter = new UniProtEntryConverter(pathway);
            result = converter.convert(uniProtkbEntry);
        } else {
            InactiveUniprotEntryConverter inactiveConverter = new InactiveUniprotEntryConverter();
            result = inactiveConverter.convert(uniProtkbEntry);
        }
        return result;
    }
}

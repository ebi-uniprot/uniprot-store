package indexer.uniprot.mapper;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import indexer.uniprot.converter.UniProtEntryConverter;

/**
 * @author lgonzales
 * @since 2019-11-12
 */
public class UniProtEntryToSolrDocument
        implements Serializable, Function<UniProtEntry, UniProtDocument> {

    private static final long serialVersionUID = -6891371730036443245L;
    private final Map<String, String> pathway;

    public UniProtEntryToSolrDocument(Map<String, String> pathway) {
        this.pathway = pathway;
    }

    @Override
    public UniProtDocument call(UniProtEntry uniProtEntry) throws Exception {
        UniProtEntryConverter converter = new UniProtEntryConverter(pathway);
        return converter.convert(uniProtEntry);
    }
}

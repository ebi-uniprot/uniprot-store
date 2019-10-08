package indexer.uniprot;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.common.SolrInputDocument;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.io.Serializable;
import java.util.HashMap;
import java.util.function.Function;

/**
 * @author lgonzales
 * @since 2019-09-30
 */
public class UniprotEntryToSolrDocumentConverter implements Function<UniProtEntry, SolrInputDocument>, Serializable {

    private static final long serialVersionUID = -6891371730036443245L;

    @Override
    public SolrInputDocument apply(UniProtEntry uniProtEntry) {
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, new HashMap<>());
        DocumentObjectBinder binder = new DocumentObjectBinder();
        UniProtDocument document = converter.convert(uniProtEntry);
        return binder.toSolrInputDocument(document);
    }

}

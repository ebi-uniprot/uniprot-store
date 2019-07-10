package uk.ac.ebi.uniprot.indexer.common.writer;

import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.Document;

import java.util.List;

/**
 * @author lgonzales
 */
public class SolrDocumentWriter<T extends Document> implements ItemWriter<T> {
    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;

    public SolrDocumentWriter(UniProtSolrOperations solrOperations, SolrCollection collection) {
        this.solrOperations = solrOperations;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends T> items) {
        this.solrOperations.saveBeans(collection.name(), items);
        this.solrOperations.softCommit(collection.name());
    }
}

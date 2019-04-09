package uk.ac.ebi.uniprot.indexer.common.writer;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.configure.Document;
import uk.ac.ebi.uniprot.indexer.configure.SolrCollection;

import java.util.List;
/**
 *
 * @author lgonzales
 */
public class SolrDocumentWriter<T extends Document> implements ItemWriter<T> {

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public SolrDocumentWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends T> items){
        this.solrTemplate.saveBeans(collection.name(), items);
        this.solrTemplate.softCommit(collection.name());
    }
}

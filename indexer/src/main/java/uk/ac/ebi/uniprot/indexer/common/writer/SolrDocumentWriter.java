package uk.ac.ebi.uniprot.indexer.common.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrCollection;
import uk.ac.ebi.uniprot.indexer.common.model.Document;

import java.util.List;
/**
 *
 * @author lgonzales
 */
public class SolrDocumentWriter<T extends Document> implements ItemWriter<T> {

    private static final Logger logger = LoggerFactory.getLogger(SolrDocumentWriter.class);

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private int pageCount = 0;

    public SolrDocumentWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends T> items){
        this.solrTemplate.saveBeans(collection.name(), items);
        this.solrTemplate.softCommit(collection.name());
        pageCount++;
        logger.info("chunck write for SolrDocumentWriter completed: "+ (pageCount * items.size()));
    }
}

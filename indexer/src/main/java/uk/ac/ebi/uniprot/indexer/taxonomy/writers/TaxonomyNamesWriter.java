package uk.ac.ebi.uniprot.indexer.taxonomy.writers;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrCollection;
import uk.ac.ebi.uniprot.indexer.taxonomy.TaxonomyDocument;

import java.util.List;

public class TaxonomyNamesWriter implements ItemWriter<TaxonomyDocument> {

    private static final Logger logger = LoggerFactory.getLogger(TaxonomyNamesWriter.class);

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private int pageCount = 0;

    public TaxonomyNamesWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends TaxonomyDocument> items){
        for (TaxonomyDocument document: items) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            solrInputDocument.addField("id",document.getId()); //TODO: use search enum that will be created
            solrInputDocument.addField("tax_id",document.getTaxId()); //TODO: use search enum that will be created
            solrInputDocument.addField("other_names",document.getOtherNames().get(0)); //TODO: use search enum that will be created
            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }
        this.solrTemplate.softCommit(collection.name());
        pageCount++;
        logger.info("chunck write for TaxonomyNamesWriter completed: "+ (pageCount * items.size()));
    }
}
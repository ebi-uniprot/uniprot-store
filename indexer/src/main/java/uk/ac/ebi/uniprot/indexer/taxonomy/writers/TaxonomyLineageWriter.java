package uk.ac.ebi.uniprot.indexer.taxonomy.writers;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.taxonomy.TaxonomyDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaxonomyLineageWriter implements ItemWriter<TaxonomyDocument> {

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public TaxonomyLineageWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends TaxonomyDocument> items) throws Exception {
        for (TaxonomyDocument document: items) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            solrInputDocument.addField("id",document.getId()); //TODO: use search enum that will be created
            Map<String,Object> fieldModifier = new HashMap<>(1);
            fieldModifier.put("set",document.getLineage());
            solrInputDocument.addField("lineage",fieldModifier); //TODO: use search enum that will be created
            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }
        this.solrTemplate.softCommit(collection.name());
    }

}

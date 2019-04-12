package uk.ac.ebi.uniprot.indexer.crossref.writers;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;

import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author sahmad
 */
public class CrossRefUniProtCountWriter implements ItemWriter<CrossRefDocument> {
    private static final String ACCESSION_STR = "accession";
    private static final String UNIPROT_ENTRY_COUNT_STR = "uniprotkb_entry_count";

    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public CrossRefUniProtCountWriter(SolrTemplate solrTemplate, SolrCollection collection){
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends CrossRefDocument> items){
        for (CrossRefDocument document: items) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            if(document.getUniprotCount() != null){
                Map<String,Object> fieldModifier = new HashMap<>(1);
                fieldModifier.put("set", document.getUniprotCount());
                solrInputDocument.addField(UNIPROT_ENTRY_COUNT_STR, fieldModifier);
            }
            solrInputDocument.addField(ACCESSION_STR, document.getAccession());
            this.solrTemplate.saveBean(collection.name(), solrInputDocument);
        }

        this.solrTemplate.softCommit(collection.name());
    }
}

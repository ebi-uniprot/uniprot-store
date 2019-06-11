package uk.ac.ebi.uniprot.indexer.proteome;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.ProteomeType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jluo
 * @date: 17 May 2019
 */

public class ProteomeDocumentWriter implements ItemWriter<ProteomeType> {
    private final ItemProcessor<ProteomeType, ProteomeDocument> itemProcessor;
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public ProteomeDocumentWriter(ItemProcessor<ProteomeType, ProteomeDocument> itemProcessor, SolrTemplate solrTemplate) {
        this.itemProcessor = itemProcessor;
        this.solrTemplate = solrTemplate;
        this.collection = SolrCollection.proteome;
    }

    @Override
    public void write(List<? extends ProteomeType> items) throws Exception {
        List<ProteomeDocument> documents = new ArrayList<>();
        for (ProteomeType proteome : items) {
            documents.add(itemProcessor.process(proteome));
        }
        solrTemplate.saveBeans(collection.name(), documents);
        solrTemplate.softCommit(collection.name());
    }

}


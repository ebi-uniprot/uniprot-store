package uk.ac.ebi.uniprot.indexer.proteome;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jluo
 * @date: 17 May 2019
 */

public class ProteomeDocumentWriter implements ItemWriter<Proteome> {
    private final ItemProcessor<Proteome, ProteomeDocument> itemProcessor;
    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;

    public ProteomeDocumentWriter(ItemProcessor<Proteome, ProteomeDocument> itemProcessor, UniProtSolrOperations solrOperations) {
        this.itemProcessor = itemProcessor;
        this.solrOperations = solrOperations;
        this.collection = SolrCollection.proteome;
    }

    @Override
    public void write(List<? extends Proteome> items) throws Exception {
        List<ProteomeDocument> documents = new ArrayList<>();
        for (Proteome proteome : items) {
            documents.add(itemProcessor.process(proteome));
        }
        solrOperations.saveBeans(collection.name(), documents);
        solrOperations.softCommit(collection.name());
    }

}


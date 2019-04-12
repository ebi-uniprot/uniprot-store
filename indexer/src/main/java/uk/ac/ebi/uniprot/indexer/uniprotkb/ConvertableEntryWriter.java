package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
public class ConvertableEntryWriter implements ItemWriter<ConvertableEntry> {
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;

    public ConvertableEntryWriter(SolrTemplate solrTemplate, SolrCollection collection) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends ConvertableEntry> convertableEntries) {
        List<UniProtDocument> uniProtDocuments = convertableEntries.stream()
                .map(ConvertableEntry::getDocument)
                .collect(Collectors.toList());
        solrTemplate.saveBeans(collection.name(), uniProtDocuments);
    }
}

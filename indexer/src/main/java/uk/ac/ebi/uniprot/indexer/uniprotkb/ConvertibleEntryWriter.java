package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.document.SolrCollection;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
public class ConvertibleEntryWriter implements ItemWriter<ConvertibleEntry> {
    private final SolrTemplate solrTemplate;
    private final SolrCollection collection;
    private StepExecution stepExecution;

    public ConvertibleEntryWriter(SolrTemplate solrTemplate, SolrCollection collection) {
        this.solrTemplate = solrTemplate;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends ConvertibleEntry> convertibleEntries) {
        // record entries we are going to try to write, in case of failure
        if (stepExecution != null) {
            recordEntries(convertibleEntries);
        }

        List<UniProtDocument> uniProtDocuments = convertibleEntries.stream()
                .map(ConvertibleEntry::getDocument)
                .collect(Collectors.toList());
        solrTemplate.saveBeans(collection.name(), uniProtDocuments);
    }

    private void recordEntries(List<? extends ConvertibleEntry> convertibleEntries) {
        // TODO: 15/04/19 find way to ensure failed entries are not written twice
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.UNIPROTKB_INDEX_FAILED_ENTRIES_CHUNK_KEY, convertibleEntries);

//        Map<String, UniProtEntry> map
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution){
        this.stepExecution = stepExecution;
    }
}

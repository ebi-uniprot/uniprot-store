package org.uniprot.store.indexer.proteome;

import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.createSuggestionMapKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author jluo
 */
public class ProteomeItemProcessor implements ItemProcessor<Proteome, ProteomeDocument> {
    private final ProteomeDocumentConverter documentConverter;
    private final ProteomeEntryAdapter entryAdapter;
    private final Map<String, SuggestDocument> suggestions;

    public ProteomeItemProcessor(
            ProteomeDocumentConverter documentConverter, ProteomeEntryAdapter entryAdapter) {
        this.documentConverter = documentConverter;
        this.entryAdapter = entryAdapter;
        this.suggestions = new HashMap<>();
    }

    @Override
    public ProteomeDocument process(Proteome source) throws Exception {
        ProteomeDocument document = documentConverter.convert(source);

        ProteomeEntry entry = entryAdapter.adaptEntry(source);
        document.proteomeStored = documentConverter.getBinaryObject(entry);

        addProteomeToSuggestMap(document.upid, document.organismSort);
        return document;
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.SUGGESTIONS_MAP, this.suggestions);
    }

    private void addProteomeToSuggestMap(String upid, String organismName) {
        if (Objects.nonNull(upid) && Objects.nonNull(organismName)) {
            String key = createSuggestionMapKey(SuggestDictionary.PROTEOME_UPID, upid);
            this.suggestions.putIfAbsent(key, createSuggestDoc(upid, organismName));
        }
    }

    private SuggestDocument createSuggestDoc(String upid, String organismName) {
        return SuggestDocument.builder()
                .id(upid)
                .value(organismName)
                .altValue(upid)
                .dictionary(SuggestDictionary.PROTEOME_UPID.name())
                .build();
    }
}

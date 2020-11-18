package org.uniprot.store.indexer.proteome;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.proteome.ProteomeType;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.createSuggestionMapKey;

/** @author jluo */
public class ProteomeDocumentProcessor implements ItemProcessor<ProteomeType, ProteomeDocument> {
    private final DocumentConverter<ProteomeType, ProteomeDocument> documentConverter;
    private final Map<String, SuggestDocument> suggestions;

    public ProteomeDocumentProcessor(
            DocumentConverter<ProteomeType, ProteomeDocument> documentConverter) {
        this.documentConverter = documentConverter;
        this.suggestions = new HashMap<>();
    }

    @Override
    public ProteomeDocument process(ProteomeType source) throws Exception {
        ProteomeDocument result = documentConverter.convert(source);
        addProteomeToMap(result.upid, result.organismSort);
        return result;
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.SUGGESTIONS_MAP, this.suggestions);
    }

    private void addProteomeToMap(String upid, String organismName) {
        if (Objects.nonNull(upid) && Objects.nonNull(organismName)) {
            String key =
                    createSuggestionMapKey(SuggestDictionary.PROTEOME_UPID, upid);
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

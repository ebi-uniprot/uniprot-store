package org.uniprot.store.indexer.proteome;

import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.createSuggestionMapKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.job.common.reader.XmlItemReader;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/** @author jluo */
public class ProteomeXmlEntryReader extends XmlItemReader<Proteome> {
    public static final String PROTEOME_ROOT_ELEMENT = "proteome";
    private Map<String, SuggestDocument> suggestions;

    public ProteomeXmlEntryReader(String filepath) {
        super(filepath, Proteome.class, PROTEOME_ROOT_ELEMENT);
        this.suggestions = new HashMap<>();
    }

    @Override
    public Proteome read() throws Exception {
        Proteome proteome = super.read();
        addProteomeToMap(proteome);
        return proteome;
    }

    @BeforeStep
    public void setStepExecution(final StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getJobExecution().getExecutionContext();
        executionContext.put(Constants.SUGGESTIONS_MAP, this.suggestions);
    }

    private void addProteomeToMap(Proteome proteome) {
        if (Objects.nonNull(proteome)) {
            String key =
                    createSuggestionMapKey(SuggestDictionary.PROTEOME_UPID, proteome.getUpid());
            this.suggestions.putIfAbsent(key, createSuggestDoc(proteome));
        }
    }

    private SuggestDocument createSuggestDoc(Proteome proteome) {
        return SuggestDocument.builder()
                .id(proteome.getUpid())
                .value(proteome.getName())
                .altValue(proteome.getUpid())
                .dictionary(SuggestDictionary.PROTEOME_UPID.name())
                .build();
    }
}

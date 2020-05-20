package org.uniprot.store.indexer.unirule;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

/**
 * @author sahmad
 * @date: 14 May 2020
 */
public class UniRuleProcessor implements ItemProcessor<UniRuleType, UniRuleDocument> {
    private final UniRuleDocumentConverter documentConverter;

    public UniRuleProcessor() {
        this.documentConverter = new UniRuleDocumentConverter();
    }

    @Override
    public UniRuleDocument process(UniRuleType uniRuleType) {
        return this.documentConverter.convert(uniRuleType);
    }
}

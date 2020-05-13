package org.uniprot.store.indexer.unirule;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.search.document.unirule.UniRuleDocument;

public class UniRuleProcessor implements ItemProcessor<UniRuleType, UniRuleDocument> {
    private final UniRuleDocumentConverter documentConverter;

    public UniRuleProcessor() {
        this.documentConverter = new UniRuleDocumentConverter();
    }

    @Override
    public UniRuleDocument process(UniRuleType uniRuleType) throws Exception {
        this.documentConverter.setProteinsAnnotatedCount(getProteinsAnnotationCount(uniRuleType));
        return this.documentConverter.convert(uniRuleType);
    }

    private Long getProteinsAnnotationCount(UniRuleType uniRuleType) {
        return ThreadLocalRandom.current().nextLong(); // FIXME
    }
}

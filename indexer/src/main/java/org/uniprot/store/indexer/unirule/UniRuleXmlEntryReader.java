package org.uniprot.store.indexer.unirule;

import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.job.common.reader.XmlItemReader;

/**
 * @author sahmad
 * @date: 12 May 2020
 */
public class UniRuleXmlEntryReader extends XmlItemReader<UniRuleType> {
    public static final String UNIRULE_ENTRY_TAG = "unirule";

    public UniRuleXmlEntryReader(String filepath) {
        super(filepath, UniRuleType.class, UNIRULE_ENTRY_TAG);
    }
}

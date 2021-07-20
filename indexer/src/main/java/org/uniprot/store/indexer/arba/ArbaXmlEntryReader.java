package org.uniprot.store.indexer.arba;

import org.uniprot.core.xml.jaxb.unirule.UniRuleType;
import org.uniprot.store.job.common.reader.XmlItemReader;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
public class ArbaXmlEntryReader extends XmlItemReader<UniRuleType> {
    public static final String UNIRULE_ENTRY_TAG = "unirule";

    public ArbaXmlEntryReader(String filepath) {
        super(filepath, UniRuleType.class, UNIRULE_ENTRY_TAG);
    }
}

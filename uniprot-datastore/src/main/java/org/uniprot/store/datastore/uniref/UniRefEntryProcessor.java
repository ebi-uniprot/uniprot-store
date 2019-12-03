package org.uniprot.store.datastore.uniref;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniref.UniRefEntryConverter;

/**
 * @author jluo
 * @date: 16 Aug 2019
 */
public class UniRefEntryProcessor implements ItemProcessor<Entry, UniRefEntry> {
    private final UniRefEntryConverter converter;

    public UniRefEntryProcessor() {
        converter = new UniRefEntryConverter();
    }

    @Override
    public UniRefEntry process(Entry item) throws Exception {
        return converter.fromXml(item);
    }
}

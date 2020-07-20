package org.uniprot.store.datastore.light.uniref;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniref.UniRefEntryLightConverter;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
public class UniRefEntryLightProcessor implements ItemProcessor<Entry, UniRefEntryLight> {
    private final UniRefEntryLightConverter converter;

    public UniRefEntryLightProcessor() {
        converter = new UniRefEntryLightConverter();
    }

    @Override
    public UniRefEntryLight process(Entry item) throws Exception {
        return converter.fromXml(item);
    }
}

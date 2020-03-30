package org.uniprot.store.datastore.uniparc;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
public class UniParcEntryProcessor implements ItemProcessor<Entry, UniParcEntry> {

    private UniParcEntryConverter converter;

    public UniParcEntryProcessor() {
        converter = new UniParcEntryConverter();
    }

    @Override
    public UniParcEntry process(Entry item) throws Exception {
        return converter.fromXml(item);
    }
}

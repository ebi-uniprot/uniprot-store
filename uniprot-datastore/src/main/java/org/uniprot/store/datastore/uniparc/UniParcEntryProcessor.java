package org.uniprot.store.datastore.uniparc;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.cv.taxonomy.TaxonomyRepo;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
public class UniParcEntryProcessor implements ItemProcessor<Entry, UniParcEntry> {

    private UniParcEntryConverter converter;

    public UniParcEntryProcessor(TaxonomyRepo taxonomyRepo) {
        converter = new UniParcEntryConverter(taxonomyRepo);
    }

    @Override
    public UniParcEntry process(Entry item) throws Exception {
        return converter.fromXml(item);
    }
}

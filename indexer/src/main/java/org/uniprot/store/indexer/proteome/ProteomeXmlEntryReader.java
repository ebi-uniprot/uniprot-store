package org.uniprot.store.indexer.proteome;

import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.common.XmlItemReader;

/**
 * @author jluo
 */
public class ProteomeXmlEntryReader extends  XmlItemReader<Proteome> {
    public static final String PROTEOME_ROOT_ELEMENT = "proteome";

    public ProteomeXmlEntryReader(String filepath) {
       super(filepath, Proteome.class, PROTEOME_ROOT_ELEMENT);
    }

}

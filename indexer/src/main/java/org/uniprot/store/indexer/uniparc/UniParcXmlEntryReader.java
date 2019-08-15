package org.uniprot.store.indexer.uniparc;

import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.job.common.reader.XmlItemReader;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
*/

public class UniParcXmlEntryReader extends  XmlItemReader<Entry> {
    public static final String UNIPARC_ROOT_ELEMENT = "entry";

    public UniParcXmlEntryReader(String filepath) {
       super(filepath, Entry.class, UNIPARC_ROOT_ELEMENT);
    }

}


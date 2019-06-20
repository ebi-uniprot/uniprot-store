package uk.ac.ebi.uniprot.indexer.uniparc;

import uk.ac.ebi.uniprot.indexer.common.XmlItemReader;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

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


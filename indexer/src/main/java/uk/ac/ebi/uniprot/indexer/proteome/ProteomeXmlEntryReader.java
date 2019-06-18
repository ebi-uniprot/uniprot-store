package uk.ac.ebi.uniprot.indexer.proteome;

import uk.ac.ebi.uniprot.indexer.common.XmlItemReader;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

/**
 * @author jluo
 */
public class ProteomeXmlEntryReader extends  XmlItemReader<Proteome> {
    public static final String PROTEOME_ROOT_ELEMENT = "proteome";

    public ProteomeXmlEntryReader(String filepath) {
       super(filepath, Proteome.class, PROTEOME_ROOT_ELEMENT);
    }

}

package org.uniprot.store.datastore.uniref;

import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.job.common.reader.XmlItemReader;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
*/

public class UniRefXmlEntryReader  extends XmlItemReader<Entry> {
	 public static final String UNIREF_ROOT_ELEMENT = "entry";
	public UniRefXmlEntryReader(String filepath) {
		 super(filepath, Entry.class, UNIREF_ROOT_ELEMENT);
	}

}


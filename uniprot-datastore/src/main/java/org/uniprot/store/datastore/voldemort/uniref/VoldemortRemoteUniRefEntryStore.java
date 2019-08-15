package org.uniprot.store.datastore.voldemort.uniref;

import org.uniprot.core.json.parser.uniref.UniRefEntryJsonConfig;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
*/

public class VoldemortRemoteUniRefEntryStore extends VoldemortRemoteJsonBinaryStore<UniRefEntry> {

	 public VoldemortRemoteUniRefEntryStore(int maxConnection, String storeName, String... voldemortUrl) {
	        super(maxConnection, storeName, voldemortUrl);
	    }

	
	@Override
	public String getStoreId(UniRefEntry entry) {
		return entry.getId().getValue();
	}

	@Override
	public ObjectMapper getStoreObjectMapper() {
		return UniRefEntryJsonConfig.getInstance().getFullObjectMapper();
	}

	@Override
	public Class<UniRefEntry> getEntryClass() {
		 return UniRefEntry.class;
	}

}


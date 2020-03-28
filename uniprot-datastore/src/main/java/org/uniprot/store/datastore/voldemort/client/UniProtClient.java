package org.uniprot.store.datastore.voldemort.client;

import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;

public interface UniProtClient extends VoldemortClient<UniProtKBEntry> {}

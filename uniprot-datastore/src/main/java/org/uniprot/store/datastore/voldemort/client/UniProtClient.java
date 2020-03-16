package org.uniprot.store.datastore.voldemort.client;

import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;

public interface UniProtClient extends VoldemortClient<UniProtkbEntry> {}

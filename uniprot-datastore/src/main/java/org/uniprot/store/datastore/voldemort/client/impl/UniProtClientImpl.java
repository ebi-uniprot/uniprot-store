package org.uniprot.store.datastore.voldemort.client.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;

public class UniProtClientImpl implements UniProtClient {
    private final VoldemortClient<UniProtkbEntry> client;

    UniProtClientImpl(VoldemortClient<UniProtkbEntry> client) {
        this.client = client;
    }

    @Override
    public Optional<UniProtkbEntry> getEntry(String accession) {
        return client.getEntry(accession);
    }

    @Override
    public List<UniProtkbEntry> getEntries(Iterable<String> accessions) {
        return client.getEntries(accessions);
    }

    @Override
    public Map<String, UniProtkbEntry> getEntryMap(Iterable<String> ids) {
        return client.getEntryMap(ids);
    }

    @Override
    public void saveEntry(UniProtkbEntry entry) {
        client.saveEntry(entry);
    }

    @Override
    public String getStoreName() {
        return client.getStoreName();
    }

    public void truncate() {
        client.truncate();
    }
}

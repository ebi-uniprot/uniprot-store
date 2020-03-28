package org.uniprot.store.datastore.voldemort.client.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;

public class UniProtClientImpl implements UniProtClient {
    private final VoldemortClient<UniProtKBEntry> client;

    UniProtClientImpl(VoldemortClient<UniProtKBEntry> client) {
        this.client = client;
    }

    @Override
    public Optional<UniProtKBEntry> getEntry(String accession) {
        return client.getEntry(accession);
    }

    @Override
    public List<UniProtKBEntry> getEntries(Iterable<String> accessions) {
        return client.getEntries(accessions);
    }

    @Override
    public Map<String, UniProtKBEntry> getEntryMap(Iterable<String> ids) {
        return client.getEntryMap(ids);
    }

    @Override
    public void saveEntry(UniProtKBEntry entry) {
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

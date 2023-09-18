package org.uniprot.store.datastore.voldemort.uniprot;

import org.uniprot.core.json.parser.uniprot.UniprotKBJsonConfig;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * This class contains methods to save Uniprot voldemort entry remotely.
 *
 * <p>Created 05/10/2017
 *
 * @author lgonzales
 */
public class VoldemortRemoteUniProtKBEntryStore
        extends VoldemortRemoteJsonBinaryStore<UniProtKBEntry> {
    public static final String UNIPROT_VOLDEMORT_URL = "uniprotVoldemortUrl";
    public static final String UNIPROT_VOLDEMORT_STORE_NAME = "uniprotVoldemortStoreName";

    @Inject
    public VoldemortRemoteUniProtKBEntryStore(
            @Named(UNIPROT_VOLDEMORT_STORE_NAME) String storeName,
            @Named(UNIPROT_VOLDEMORT_URL) String voldemortUrl) {
        super(storeName, voldemortUrl);
    }

    public VoldemortRemoteUniProtKBEntryStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniProtKBEntryStore(
            int maxConnection,
            boolean brotliEnabled,
            int brotliLevel,
            String storeName,
            String... voldemortUrl) {
        super(maxConnection, brotliEnabled, brotliLevel, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniProtKBEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniprotKBJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniProtKBEntry> getEntryClass() {
        return UniProtKBEntry.class;
    }
}

package org.uniprot.store.datastore.voldemort.uniprot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.uniprot.core.json.parser.uniprot.UniprotJsonConfig;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.UniProtEntryType;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteCachingJsonBinaryStore;

/**
 * This class contains methods to save UniProtKB entries to a remote Voldemort store, whilst caching
 * Swiss-Prot entries locally.
 *
 * <p>Created 27/02/2020
 *
 * @author Edd
 */
public class VoldemortRemoteCachingUniProtKBEntryStore
        extends VoldemortRemoteCachingJsonBinaryStore<UniProtEntry> {
    public static final String UNIPROT_VOLDEMORT_URL = "uniprotVoldemortUrl";
    public static final String UNIPROT_VOLDEMORT_STORE_NAME = "uniprotVoldemortStoreName";

    @Inject
    public VoldemortRemoteCachingUniProtKBEntryStore(
            @Named(UNIPROT_VOLDEMORT_STORE_NAME) String storeName,
            @Named(UNIPROT_VOLDEMORT_URL) String voldemortUrl) {
        super(storeName, voldemortUrl);
    }

    public VoldemortRemoteCachingUniProtKBEntryStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    @Override
    protected boolean isCacheable(UniProtEntry item) {
        return item.getEntryType().equals(UniProtEntryType.SWISSPROT);
    }

    @Override
    public String getStoreId(UniProtEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniprotJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniProtEntry> getEntryClass() {
        return UniProtEntry.class;
    }
}

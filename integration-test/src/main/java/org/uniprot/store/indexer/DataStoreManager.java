package org.uniprot.store.indexer;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.search.document.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class DataStoreManager {
    public enum StoreType {
        UNIPROT, INACTIVE_UNIPROT, UNIPARC, UNIREF, CROSSREF, PROTEOME, DISEASE, TAXONOMY, GENECENTRIC,
        KEYWORD, LITERATURE
    }

    private static final Logger LOGGER = getLogger(DataStoreManager.class);
    private final SolrDataStoreManager solrDataStoreManager;
    private final Map<StoreType, SolrClient> solrClientMap = new HashMap<>();
    private final Map<StoreType, UniProtStoreClient> storeMap = new HashMap<>();
    private final Map<StoreType, DocumentConverter> docConverterMap = new HashMap<>();

    public DataStoreManager(SolrDataStoreManager solrDataStoreManager) {
        this.solrDataStoreManager = solrDataStoreManager;
    }

    public void close() {
        for (SolrClient client : solrClientMap.values()) {
            try {
                client.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        solrDataStoreManager.cleanUp();
    }

    public <T> void save(StoreType storeType, List<T> entries) {
        saveToStore(storeType, entries);
        saveEntriesInSolr(storeType, entries);
    }

    public <T> void save(StoreType storeType, T... entries) {
        saveToStore(storeType, asList(entries));
        saveEntriesInSolr(storeType, asList(entries));
    }

    public void addSolrClient(StoreType storeType, ClosableEmbeddedSolrClient client) {
        solrClientMap.put(storeType, client);
    }

    public void addStore(StoreType storeType, UniProtStoreClient storeClient) {
        storeMap.put(storeType, storeClient);
    }

    public void addDocConverter(StoreType storeType, DocumentConverter converter) {
        docConverterMap.put(storeType, converter);
    }

    @SuppressWarnings("unchecked")
    public <T> void saveToStore(StoreType storeType, List<T> entries) {
        UniProtStoreClient storeClient = getStore(storeType);
        if (storeClient == null) {
            return;
        }
        int count = 0;
        for (Object o : entries) {
            try {
                storeClient.saveEntry(o);
                count++;
            } catch (Exception e) {
                LOGGER.debug("Trying to add entry {} to data store again but a problem was encountered -- skipping", o);
            }
        }

        LOGGER.debug("Added {} entries to data store", count);
    }

    public <T> void saveToStore(StoreType storeType, T... entries) {
        saveToStore(storeType, asList(entries));
    }

    public SolrClient getSolrClient(StoreType storeType) {
        return solrClientMap.get(storeType);
    }

    public QueryResponse querySolr(StoreType storeType, String query) throws IOException, SolrServerException {
        return solrClientMap.get(storeType).query(new SolrQuery(query));
    }

    public <T> void saveDocs(StoreType storeType, List<T> docs) {
        SolrClient client = getSolrClient(storeType);
        try {
            for (T doc : docs) {
                client.addBean(doc);
            }
            client.commit();
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException(e);
        }
        LOGGER.debug("Added {} beans to Solr", docs.size());
    }

    public <T> void saveDocs(StoreType storeType, T... docs) {
        saveDocs(storeType, asList(docs));
    }

    @SuppressWarnings("unchecked")
    public <T, D extends Document> void saveEntriesInSolr(StoreType storeType, List<T> entries) {
        DocumentConverter<T, D> documentConverter = docConverterMap.get(storeType);
        SolrClient client = solrClientMap.get(storeType);
        List<D> docs = entries.stream().map(documentConverter::convert)
                .collect(Collectors.toList());
        try {
            client.addBeans(docs);
            client.commit();
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public <T> void saveEntriesInSolr(StoreType storeType, T... entries) {
        saveEntriesInSolr(storeType, asList(entries));
    }

    private UniProtStoreClient getStore(StoreType storeType) {
        return storeMap.get(storeType);
    }

    public <T> List<T> getStoreEntries(StoreType storeType, String... entries) {
        return getStoreEntries(storeType, asList(entries));
    }

    public <S, T> List<T> getStoreEntries(StoreType storeType, List<String> entries) {
        return getStore(storeType).getEntries(entries);
    }

    public void cleanSolr(StoreType storeType) {
        try {
            SolrClient solrClient = solrClientMap.get(storeType);
            solrClient.deleteByQuery("*:*");
            solrClient.commit();
        } catch (SolrServerException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

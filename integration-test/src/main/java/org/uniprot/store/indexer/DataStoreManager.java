package org.uniprot.store.indexer;

import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.core.CoreContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.springframework.util.FileSystemUtils;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.Document;
import org.uniprot.store.search.document.DocumentConverter;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class DataStoreManager implements AfterAllCallback, BeforeAllCallback {

    public enum StoreType {
        UNIPROT,
        INACTIVE_UNIPROT,
        UNIPARC,
        UNIPARC_LIGHT,
        UNIPARC_CROSS_REFERENCE,
        UNIREF_LIGHT,
        UNIREF_MEMBER,
        CROSSREF,
        PROTEOME,
        DISEASE,
        TAXONOMY,
        GENECENTRIC,
        KEYWORD,
        LITERATURE,
        SUBCELLULAR_LOCATION,
        SUGGEST,
        PUBLICATION,
        UNIRULE,
        HELP,
        ARBA,
        GOOGLE_PROTLM
    }

    private static final String SOLR_SYSTEM_PROPERTIES = "solr-system.properties";
    private static final Logger LOGGER = getLogger(DataStoreManager.class);
    private final Map<StoreType, ClosableEmbeddedSolrClient> solrClientMap = new HashMap<>();
    private final Map<StoreType, UniProtStoreClient> storeMap = new HashMap<>();
    private final Map<StoreType, DocumentConverter> docConverterMap = new HashMap<>();

    private Path temporaryFolder;
    private CoreContainer container;

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        loadPropertiesAndSetAsSystemProperties();
        temporaryFolder = Files.createTempDirectory("solr");
        System.setProperty("solr.data.home", temporaryFolder.toString());
        Path solrHome = Paths.get(System.getProperty(ClosableEmbeddedSolrClient.SOLR_HOME));
        container = new CoreContainer(solrHome, new Properties());
        container.load();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        close();
        System.setProperty("solr.data.home", "");
        FileSystemUtils.deleteRecursively(temporaryFolder);
    }

    private static void loadPropertiesAndSetAsSystemProperties() throws IOException {
        Properties properties = new Properties();
        InputStream propertiesStream =
                DataStoreManager.class.getClassLoader().getResourceAsStream(SOLR_SYSTEM_PROPERTIES);
        properties.load(propertiesStream);

        for (String property : properties.stringPropertyNames()) {
            System.out.println(property + "\t" + properties.getProperty(property));
            System.setProperty(property, properties.getProperty(property));
        }
    }

    public void close() {
        for (SolrClient client : solrClientMap.values()) {
            try {
                client.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public <T> void save(StoreType storeType, List<T> entries) {
        saveToStore(storeType, entries);
        saveEntriesInSolr(storeType, entries);
    }

    public <T> void save(StoreType storeType, T... entries) {
        saveToStore(storeType, asList(entries));
        saveEntriesInSolr(storeType, asList(entries));
    }

    public void addSolrClient(StoreType storeType, SolrCollection collection) {
        ClosableEmbeddedSolrClient solrClient =
                new ClosableEmbeddedSolrClient(container, collection);
        solrClientMap.put(storeType, solrClient);
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
                LOGGER.debug(
                        "Trying to add entry {} to data store again but a problem was encountered -- skipping",
                        o);
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

    public QueryResponse querySolr(StoreType storeType, String query)
            throws IOException, SolrServerException {
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
        List<D> docs =
                entries.stream().map(documentConverter::convert).collect(Collectors.toList());
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
            ClosableEmbeddedSolrClient solrClient = solrClientMap.get(storeType);
            solrClient.deleteByQuery("*:*");
            solrClient.commit();
        } catch (Exception e) {
            LOGGER.warn("Unable to clean solr data for " + storeType.name(), e);
        }
    }

    public void cleanStore(StoreType storeType) {
        UniProtStoreClient storeClient = storeMap.get(storeType);
        if (storeClient != null) {
            storeClient.truncate();
        }
    }
}

package org.uniprot.store.indexer.search.proteome;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.core.xml.proteome.ProteomeConverter;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.indexer.proteome.ProteomeDocumentConverter;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

class ProteomeSearchEngine extends AbstractSearchEngine<Proteome> {

    private final ProteomeConverter entryConverter;
    private static final String SEARCH_ENGINE_NAME = "proteome";
    private static final String TAXONOMY_FILE_NAME = "taxonomy/taxonomy.dat";
    private static final ProteomeDocumentConverter DOCUMENT_PRODUCER = createDocumentProducer();

    public ProteomeSearchEngine() {
        super(SEARCH_ENGINE_NAME, DOCUMENT_PRODUCER);
        this.entryConverter = new ProteomeConverter();
    }

    private static ProteomeDocumentConverter createDocumentProducer() {
        TaxonomyRepo taxRepo = createTaxRepo();
        return new ProteomeDocumentConverter(taxRepo);
    }

    private static TaxonomyRepo createTaxRepo() {
        try {
            URL url = ClassLoader.getSystemClassLoader().getResource(TAXONOMY_FILE_NAME);
            File taxonomicFile = new File(url.toURI());
            FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
            return new TaxonomyMapRepo(taxonomicNodeIterable);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void indexEntry(Proteome entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Entry is null");
        }

        ProteomeDocument document = DOCUMENT_PRODUCER.convert(entry);
        ProteomeEntry proteomeEntry = entryConverter.fromXml(entry);
        document.proteomeStored = DOCUMENT_PRODUCER.getBinaryObject(proteomeEntry);

        saveDocument(document);
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.PROTEOME);
    }

    @Override
    protected String identifierField() {
        return getSearchFieldConfig().getSearchFieldItemByName("upid").getFieldName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return getSearchFieldConfig().getSearchFieldItemByName("upid").getFieldName()
                + ":"
                + entryId;
    }
}

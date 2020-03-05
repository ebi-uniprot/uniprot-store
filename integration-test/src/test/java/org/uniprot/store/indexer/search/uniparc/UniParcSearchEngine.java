package org.uniprot.store.indexer.search.uniparc;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.factory.UniProtDataType;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.uniparc.UniParcDocumentConverter;
import org.uniprot.store.job.common.converter.DocumentConverter;

/** Concrete implementation of the UniParce search engine */
class UniParcSearchEngine extends AbstractSearchEngine<Entry> {
    private static final String SEARCH_ENGINE_NAME = "uniparc";
    private static final String TAXONOMY_FILE_NAME = "taxonomy/taxonomy.dat";
    private static final DocumentConverter<Entry, ?> DOCUMENT_PRODUCER = createDocumentProducer();

    public UniParcSearchEngine() {
        super(SEARCH_ENGINE_NAME, DOCUMENT_PRODUCER);
    }

    private static DocumentConverter<Entry, ?> createDocumentProducer() {
        TaxonomyRepo taxRepo = createTaxRepo();
        return new UniParcDocumentConverter(taxRepo);
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
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIPARC);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected String identifierField() {
        return getSearchFieldConfig().getSearchFieldItemByName("upi").getFieldName();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return getSearchFieldConfig().getSearchFieldItemByName("upid").getFieldName()
                + ":"
                + entryId;
    }
}

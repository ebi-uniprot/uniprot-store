package org.uniprot.store.indexer.search.uniparc;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.uniparc.UniParcDocumentConverter;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.field.UniParcField;

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

    @SuppressWarnings("rawtypes")
    @Override
    protected Enum identifierField() {
        return UniParcField.Search.upi;
    }

    @Override
    protected String identifierQuery(String entryId) {
        return UniParcField.Search.upid.name() + ":" + entryId;
    }
}

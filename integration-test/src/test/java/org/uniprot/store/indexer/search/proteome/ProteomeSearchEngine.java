package org.uniprot.store.indexer.search.proteome;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.core.xml.jaxb.proteome.Proteome;
import org.uniprot.store.indexer.proteome.ProteomeEntryConverter;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.field.ProteomeField;

class ProteomeSearchEngine extends AbstractSearchEngine<Proteome> {

    private static final String SEARCH_ENGINE_NAME = "proteome";
    private static final String TAXONOMY_FILE_NAME = "taxonomy/taxonomy.dat";
    private static final DocumentConverter<Proteome, ?> DOCUMENT_PRODUCER =
            createDocumentProducer();

    public ProteomeSearchEngine() {
        super(SEARCH_ENGINE_NAME, DOCUMENT_PRODUCER);
    }

    private static DocumentConverter<Proteome, ?> createDocumentProducer() {
        TaxonomyRepo taxRepo = createTaxRepo();
        return new ProteomeEntryConverter(taxRepo);
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
    protected String identifierField() {
        return ProteomeField.Search.upid.name();
    }

    @Override
    protected String identifierQuery(String entryId) {
        return ProteomeField.Search.upid.name() + ":" + entryId;
    }
}

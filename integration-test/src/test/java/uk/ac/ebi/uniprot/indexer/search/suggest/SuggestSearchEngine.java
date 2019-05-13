package uk.ac.ebi.uniprot.indexer.search.suggest;

import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.proteome.ProteomeEntryConverter;
import uk.ac.ebi.uniprot.indexer.search.AbstractSearchEngine;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.search.field.SuggestField;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class SuggestSearchEngine extends AbstractSearchEngine<Proteome> {

    private static final String SEARCH_ENGINE_NAME = "suggest";
    private static final DocumentConverter<Proteome, ?> DOCUMENT_PRODUCER = createDocumentProducer();

    public SuggestSearchEngine() {
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
    protected Enum identifierField() {
        return SuggestField.Search.id;
    }

    @Override
    protected String identifierQuery(String entryId) {
        return SuggestField.Search.id + ":" + entryId;
    }

    private enum

}

package uk.ac.ebi.uniprot.indexer.search.uniparc;



import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.search.AbstractSearchEngine;
import uk.ac.ebi.uniprot.indexer.uniparc.UniParcDocumentConverter;
import uk.ac.ebi.uniprot.search.field.UniParcField;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

/**
 * Concrete implementation of the UniParce search engine
 */
public class UniParcSearchEngine extends AbstractSearchEngine<Entry> {
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
    	}catch(URISyntaxException e) {
    		throw new RuntimeException (e);
    	}
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected Enum identifierField() {
       return UniParcField.Search.upi;
    }

	@Override
	protected String identifierQuery(String entryId) {
		 return UniParcField.Search.upid.name() +":" + entryId;
	}
}
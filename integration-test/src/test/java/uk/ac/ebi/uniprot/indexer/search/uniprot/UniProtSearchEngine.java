package uk.ac.ebi.uniprot.indexer.search.uniprot;


import uk.ac.ebi.uniprot.cv.chebi.ChebiRepo;
import uk.ac.ebi.uniprot.cv.chebi.ChebiRepoFactory;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.search.AbstractSearchEngine;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprotkb.processor.UniProtEntryConverter;
import uk.ac.ebi.uniprot.search.field.UniProtField;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

/**
 * Concrete implementation of the UniProt search engine
 */
public class UniProtSearchEngine extends AbstractSearchEngine<UniProtEntry> {
    private static final String SEARCH_ENGINE_NAME = "uniprot";
    private static final String TAXONOMY_FILE_NAME = "taxonomy/taxonomy.dat";

    public UniProtSearchEngine() {
        super(SEARCH_ENGINE_NAME, TestDocumentProducer.createDefault());
    }

    public UniProtSearchEngine(DocumentConverter<UniProtEntry, ?> documentProducer) {
        super(SEARCH_ENGINE_NAME, documentProducer);
    }

    @Override
    protected void before() throws Throwable {
        setRequiredProperties();
        super.before();
    }

    private void setRequiredProperties() {
        System.setProperty("uniprot.bdb.base.location", indexHome.getAbsolutePath() + "/bdb/uniprot/data");
        System.setProperty("uniprot.bdb.test.base.location", indexHome.getAbsolutePath() + "/bdb/it_uniprot/data");
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "accession_id:" + entryId;
    }

    @Override
    protected Enum identifierField() {
        return UniProtField.Search.accession_id;
    }

    static class TestDocumentProducer {
        static DocumentConverter<UniProtEntry, ?> createDefault() {
            return new TestDocumentProducer().create();
        }

        DocumentConverter<UniProtEntry, ?> create() {
            try {
                TaxonomyRepo taxRepo = createTaxRepo();
                GoRelationRepo goRelation = createGoRelationRepo();

                return new UniProtEntryConverter(taxRepo, goRelation,
                                                 createPathwayRepo(),
                                                 createChebiRepo(),
                                                 new HashMap<>());
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Unable to access the taxonomy file location");
            }
        }

        private ChebiRepo createChebiRepo() {
            return ChebiRepoFactory.get("chebi.obo");
        }

        TaxonomyRepo createTaxRepo() throws URISyntaxException {
            URL url = ClassLoader.getSystemClassLoader().getResource(TAXONOMY_FILE_NAME);
            File taxonomicFile = new File(url.toURI());
            FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
            return new TaxonomyMapRepo(taxonomicNodeIterable);
        }

        GoRelationRepo createGoRelationRepo() {
            String gotermPath = ClassLoader.getSystemClassLoader().getResource("goterm").getFile();
            return GoRelationFileRepo.create(new GoRelationFileReader(gotermPath),
                                             new GoTermFileReader(gotermPath));
        }

        PathwayRepo createPathwayRepo() {
            return new PathwayFileRepo("unipathway.txt");
        }
    }
}

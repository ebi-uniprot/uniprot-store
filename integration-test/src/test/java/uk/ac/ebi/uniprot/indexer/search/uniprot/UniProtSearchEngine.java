package uk.ac.ebi.uniprot.indexer.search.uniprot;


import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.DocumentProducer;
import uk.ac.ebi.uniprot.indexer.search.AbstractSearchEngine;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.search.field.UniProtField;
import uk.ac.ebi.uniprot.indexer.document.impl.UniprotEntryConverter;
import uk.ac.ebi.uniprot.indexer.document.impl.UniprotEntryDocumentProducer;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Concrete implementation of the UniProt search engine
 */
public class UniProtSearchEngine extends AbstractSearchEngine<UniProtEntry> {
    private static final String SEARCH_ENGINE_NAME = "uniprot";
    private static final String TAXONOMY_FILE_NAME = "it/taxonomy/taxonomy.dat";

    public UniProtSearchEngine() {
        super(SEARCH_ENGINE_NAME, TestDocumentProducer.createDefault());
    }

    public UniProtSearchEngine(DocumentProducer<UniProtEntry> documentProducer) {
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
   //     System.setProperty("uniprot.suggester.dir", "/Users/jluo/projects/github/uniprot-indexer/integration-test/src/test/resources/it/uniprot/suggestions/");
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "accession_id:" +entryId;
    }

    @Override
    protected Enum identifierField() {
        return UniProtField.Search.accession_id;
    }

    static class TestDocumentProducer {
        static DocumentProducer<UniProtEntry> createDefault() {
            return new TestDocumentProducer().create();
        }

        DocumentProducer<UniProtEntry> create() {
            try {
                TaxonomyRepo taxRepo = createTaxRepo();
                GoRelationRepo goRelation = createGoRelationRepo();
             //   UniProtUniRefMap uniProtUniRefMapDir = createUniProtUniRefMap();

                return new UniprotEntryDocumentProducer(new UniprotEntryConverter(taxRepo, goRelation, createKeywordRepo(),
                		createPathwayRepo()
                		));
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Unable to access the taxonomy file location");
            }
        }

        TaxonomyRepo createTaxRepo() throws URISyntaxException {
            URL url = ClassLoader.getSystemClassLoader().getResource(TAXONOMY_FILE_NAME);
            File taxonomicFile = new File(url.toURI());
            FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
            return new TaxonomyMapRepo(taxonomicNodeIterable);
        }

        GoRelationRepo createGoRelationRepo() {
            String gotermPath = ClassLoader.getSystemClassLoader().getResource("it/goterm").getFile();
            return GoRelationFileRepo.create(new GoRelationFileReader(gotermPath),
                                             new GoTermFileReader(gotermPath));
        }

//        UniProtUniRefMap createUniProtUniRefMap() {
//            return UniProtUniRefMap.builder(false)
//                    .withUniRef50(createMap())
//                    .withUniRef90(createMap())
//                    .withUniRef100(createMap())
//                    .build();
//
//        }
        
        KeywordRepo createKeywordRepo() {
        	return new KeywordFileRepo("keywlist.txt");
        }
        
        PathwayRepo createPathwayRepo() {
        	return new PathwayFileRepo("unipathway.txt");
        }
//        private ChronicleMap<String, String> createMap() {
//            return ChronicleMap
//                    .of(String.class, String.class)
//                    .averageKey("AVERAGE_KEY")
//                    .averageValue("AVERAGE_VALUE")
//                    .entries(10)
//                    .create();
//        }
    }
}

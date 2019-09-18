package org.uniprot.store.indexer.search.uniprot;


import org.junit.jupiter.api.extension.ExtensionContext;
import org.uniprot.core.cv.chebi.ChebiRepo;
import org.uniprot.core.cv.chebi.ChebiRepoFactory;
import org.uniprot.core.cv.ec.ECRepo;
import org.uniprot.core.cv.ec.ECRepoFactory;
import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyMapRepo;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.uniprot.go.GoRelationFileReader;
import org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTermFileReader;
import org.uniprot.store.indexer.uniprot.pathway.PathwayFileRepo;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverter;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.field.UniProtField;

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
    public void beforeAll(ExtensionContext context) {
        setRequiredProperties();
        super.beforeAll(context);
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
                                                 createECRepo(),
                                                 new HashMap<>());
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Unable to access the taxonomy file location");
            }
        }

        private ECRepo createECRepo() {
            return ECRepoFactory.get("./");
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

package org.uniprot.store.indexer.search.uniprot;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.cv.chebi.ChebiRepo;
import org.uniprot.cv.chebi.ChebiRepoFactory;
import org.uniprot.cv.ec.ECRepo;
import org.uniprot.cv.ec.ECRepoFactory;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.indexer.search.AbstractSearchEngine;
import org.uniprot.store.indexer.uniprot.go.GoRelationFileReader;
import org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTermFileReader;
import org.uniprot.store.indexer.uniprot.pathway.PathwayFileRepo;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverter;
import org.uniprot.store.search.document.DocumentConverter;

/** Concrete implementation of the UniProt search engine */
public class UniProtSearchEngine extends AbstractSearchEngine<UniProtKBEntry> {
    private static final String SEARCH_ENGINE_NAME = "uniprot";
    private static final String TAXONOMY_FILE_NAME = "taxonomy/taxonomy.dat";

    public UniProtSearchEngine() {
        super(SEARCH_ENGINE_NAME, TestDocumentProducer.createDefault());
    }

    public UniProtSearchEngine(DocumentConverter<UniProtKBEntry, ?> documentProducer) {
        super(SEARCH_ENGINE_NAME, documentProducer);
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        setRequiredProperties();
        super.beforeAll(context);
    }

    private void setRequiredProperties() {
        System.setProperty(
                "uniprot.bdb.base.location", indexHome.getAbsolutePath() + "/bdb/uniprot/data");
        System.setProperty(
                "uniprot.bdb.test.base.location",
                indexHome.getAbsolutePath() + "/bdb/it_uniprot/data");
        System.setProperty("solr.allow.unsafe.resourceloading", "true");
    }

    @Override
    protected SearchFieldConfig getSearchFieldConfig() {
        return SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.UNIPROTKB);
    }

    @Override
    protected String identifierQuery(String entryId) {
        return "accession_id:" + entryId;
    }

    @Override
    protected String identifierField() {
        return getSearchFieldConfig().getCorrespondingSortField("accession").getFieldName();
    }

    static class TestDocumentProducer {
        static DocumentConverter<UniProtKBEntry, ?> createDefault() {
            return new TestDocumentProducer().create();
        }

        DocumentConverter<UniProtKBEntry, ?> create() {
            try {
                TaxonomyRepo taxRepo = createTaxRepo();
                GoRelationRepo goRelation = createGoRelationRepo();

                return new UniProtEntryConverter(
                        taxRepo,
                        goRelation,
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
            return GoRelationFileRepo.create(
                    new GoRelationFileReader(gotermPath), new GoTermFileReader(gotermPath));
        }

        PathwayRepo createPathwayRepo() {
            return new PathwayFileRepo("unipathway.txt");
        }
    }
}

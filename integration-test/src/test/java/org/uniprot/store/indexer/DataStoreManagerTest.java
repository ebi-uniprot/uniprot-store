package org.uniprot.store.indexer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.cv.chebi.ChebiRepo;
import org.uniprot.core.cv.ec.ECRepo;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.uniprot.mockers.*;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

class DataStoreManagerTest {
    private static final String P12345 = "P12345";

    @RegisterExtension static DataStoreManager storeManager = new DataStoreManager();

    @BeforeAll
    static void setUp() {
        try {

            storeManager.addSolrClient(DataStoreManager.StoreType.UNIPROT, SolrCollection.uniprot);

            //    UUWStoreClient storeClient = new
            // FakeStoreClient(VoldemortInMemoryUniprotEntryStore
            //             .getInstance("avro-uniprot"));
            //     storeManager.addVoldemort(DataStoreManager.StoreType.UNIPROT, storeClient);
            ChebiRepo chebiRepoMock = mock(ChebiRepo.class);
            storeManager.addDocConverter(
                    DataStoreManager.StoreType.UNIPROT,
                    new UniProtEntryConverter(
                            TaxonomyRepoMocker.getTaxonomyRepo(),
                            GoRelationsRepoMocker.getGoRelationRepo(),
                            PathwayRepoMocker.getPathwayRepo(),
                            chebiRepoMock,
                            mock(ECRepo.class),
                            new HashMap<>()));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Error to setup DataStoreManagerTest", e);
        }
    }

    //    private static UniProtUniRefMap uniprotUniRefMap() {
    //    	return  UniProtUniRefMap.builder(true).build();
    //
    //    }
    @AfterEach
    void cleanUp() {
        storeManager.cleanSolr(DataStoreManager.StoreType.UNIPROT);
    }

    // getEntry -------------------

    @Test
    void canAddAndSearchDocumentsInSolr() throws IOException, SolrServerException {
        storeManager.saveDocs(
                DataStoreManager.StoreType.UNIPROT, UniProtDocMocker.createDoc(P12345));
        QueryResponse response =
                storeManager.querySolr(DataStoreManager.StoreType.UNIPROT, "accession:P12345");
        List<String> results =
                response.getBeans(UniProtDocument.class).stream()
                        .map(doc -> doc.accession)
                        .collect(Collectors.toList());
        assertThat(results, Matchers.contains(P12345));
    }

    @Test
    void canAddEntriesAndSearchDocumentsInSolr() throws IOException, SolrServerException {
        storeManager.saveDocs(
                DataStoreManager.StoreType.UNIPROT, UniProtDocMocker.createDoc(P12345));
        QueryResponse response =
                storeManager.querySolr(DataStoreManager.StoreType.UNIPROT, "accession:P12345");
        List<String> results =
                response.getBeans(UniProtDocument.class).stream()
                        .map(doc -> doc.accession)
                        .collect(Collectors.toList());
        assertThat(results, Matchers.contains(P12345));
    }

    @Disabled
    @Test
    void canAddAndFetchEntriesInSolr() throws IOException, SolrServerException {
        UniProtEntry entry = UniProtEntryMocker.create(UniProtEntryMocker.Type.SP);
        String accession = entry.getPrimaryAccession().getValue();
        storeManager.saveEntriesInSolr(DataStoreManager.StoreType.UNIPROT, entry);
        QueryResponse response = storeManager.querySolr(DataStoreManager.StoreType.UNIPROT, "*:*");
        List<String> results =
                response.getBeans(UniProtDocument.class).stream()
                        .map(doc -> doc.accession)
                        .collect(Collectors.toList());
        assertThat(results, Matchers.contains(accession));
    }

    //    @Test
    //    void canAddAndFetchEntriesInVoldemort() {
    //        UniProtEntry entry = UniProtEntryMocker.create(UniProtEntryMocker.Type.SP);
    //        String accession = entry.getPrimaryAccession().getValue();
    //        storeManager.saveToVoldemort(DataStoreManager.StoreType.UNIPROT, entry);
    //        List<UniProtEntry> voldemortEntries =
    // storeManager.getVoldemortEntries(DataStoreManager.StoreType.UNIPROT, accession);
    //        assertThat(voldemortEntries, hasSize(1));
    //        assertThat(voldemortEntries.get(0), Matchers.is(entry));
    //    }
    //
    //    @Test
    //    void canAddAndFetchEntriesInSolrAndVoldemort() throws IOException, SolrServerException {
    //        UniProtEntry entry = UniProtEntryMocker.create(UniProtEntryMocker.Type.SP);
    //        String accession = entry.getPrimaryAccession().getValue();
    //        storeManager.save(DataStoreManager.StoreType.UNIPROT, entry);
    //
    //        QueryResponse response = storeManager.querySolr(DataStoreManager.StoreType.UNIPROT,
    // "*:*");
    //        List<String> results = response.getBeans(UniProtDocument.class).stream().map(doc ->
    // doc.accession)
    //                .collect(Collectors.toList());
    //        assertThat(results, Matchers.contains(accession));
    //
    //        List<UniProtEntry> voldemortEntries =
    // storeManager.getVoldemortEntries(DataStoreManager.StoreType.UNIPROT, accession);
    //        assertThat(voldemortEntries, hasSize(1));
    //        assertThat(voldemortEntries.get(0), Matchers.is(entry));
    //    }
    //
    //    private static class FakeStoreClient extends UUWStoreClient<UniProtEntry> {
    //
    //        FakeStoreClient(VoldemortClient<UniProtEntry> client) {
    //            super(client);
    //        }
    //    }
}

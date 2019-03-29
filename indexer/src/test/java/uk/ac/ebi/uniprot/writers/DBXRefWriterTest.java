package uk.ac.ebi.uniprot.writers;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.uniprot.api.common.repository.DataStoreManager;
import uk.ac.ebi.uniprot.api.common.repository.search.ClosableEmbeddedSolrClient;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrCollection;
import uk.ac.ebi.uniprot.api.common.repository.search.SolrDataStoreManager;
import uk.ac.ebi.uniprot.models.DBXRef;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DBXRefWriterTest {
    private static DBXRefWriter dbxRefWriter;
    private static ClosableEmbeddedSolrClient solrClient;
    private static DataStoreManager storeManager;
    private static String random;

    @BeforeAll
    static void setSolrClient() throws IOException {
        random = UUID.randomUUID().toString().substring(0, 5);
        SolrDataStoreManager solrDM = new SolrDataStoreManager();
        solrClient = new ClosableEmbeddedSolrClient(SolrCollection.crossref);
        dbxRefWriter = new DBXRefWriter(solrClient);
        storeManager = new DataStoreManager(solrDM);
        storeManager.addSolrClient(DataStoreManager.StoreType.CROSSREF, solrClient);
    }

    @AfterAll
    static void stopSolrClient() {
       storeManager.cleanSolr(DataStoreManager.StoreType.CROSSREF);
    }

    @Test
    void testWriteCrossRefs() throws Exception {
        List<DBXRef> dbxrefList = IntStream.range(0, 10).mapToObj(i -> createDBXRef(i)).collect(Collectors.toList());
        // write the cross refs to the solr
        dbxRefWriter.write(dbxrefList);
        // get the cross refs and verify
        QueryResponse response = storeManager.querySolr(DataStoreManager.StoreType.CROSSREF, "*:*");
        assertEquals(0, response.getStatus());
        List<DBXRef> results = response.getBeans(DBXRef.class);
        assertEquals(dbxrefList.size(), results.size());
        results.stream().forEach(dbXref -> verifyDBXRef(dbXref));
    }

    private void verifyDBXRef(DBXRef dbxRef) {
        assertNotNull(dbxRef.getAccession(), "Accession is null");
        assertNotNull(dbxRef.getAbbrev(), "Abbrev is null");
        assertNotNull(dbxRef.getName(), "Name is null");
        assertNotNull(dbxRef.getPubMedId(), "PUBMED ID is null");
        assertNotNull(dbxRef.getDoiId(), "DOI Id is null");
        assertNotNull(dbxRef.getLinkType(), "Link Type is null");
        assertNotNull(dbxRef.getServer(), "Server is null");
        assertNotNull(dbxRef.getDbUrl(), "DB URL is null");
        assertNotNull(dbxRef.getCategoryFacet(), "Category is null");
    }

    private DBXRef createDBXRef(int suffix){
        String ac = random + "-AC-" + suffix;
        String ab = random + "-AB-" + suffix;
        String nm = random + "-NM-" + suffix;
        String pb = random + "-PB-" + suffix;
        String di = random + "-DI-" + suffix;
        String lt = random + "-LT-" + suffix;
        String sr = random + "-SR-" + suffix;
        String du = random + "-DU-" + suffix;
        String ct = random + "-CT-" + suffix;

        DBXRef.DBXRefBuilder builder = new DBXRef.DBXRefBuilder();
        builder.abbr(ab).accession(ac).category(ct).dbUrl(du);
        builder.doiId(di).linkType(lt).name(nm).pubMedId(pb).server(sr);
        return builder.build();
    }
}

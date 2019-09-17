package org.uniprot.store.datastore.voldemort.uniprot;

import com.codahale.metrics.Counter;
import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.uniprot.store.datastore.voldemort.MetricsUtil;

import static org.junit.jupiter.api.Assertions.*;

public class VoldemortUniprotEntryStoreBuilderTest {

    private String storeName = "avro-uniprot";

    @BeforeEach
    @AfterEach
    public void cleanIndexFiles() throws Exception{
        for (int i=0;i<=10;i++) {
            new File(storeName+".parse.fail."+i+".txt").delete();
            new File(storeName+".storing.fail."+i+".txt").delete();
        }
        MetricsUtil.resetMetrics();
        Field instance = VoldemortInMemoryUniprotEntryStore.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
    }

    @Test
    public void testSuccessVoldemortUniprotBuild(){
        VoldemortInMemoryUniprotEntryStore voldemortInMemoryEntryStore = VoldemortInMemoryUniprotEntryStore.getInstance(storeName);
        try{
            VoldemortUniprotEntryStoreBuilder voldemortEntryStoreBuiler = new VoldemortUniprotEntryStoreBuilder(voldemortInMemoryEntryStore, storeName);
            URL keywordFile = VoldemortInMemoryUniprotEntryStoreTest.class.getResource("/uniprot/keywlist.txt");
            assertNotNull(keywordFile);
            URL goTermsPath = getClass().getClassLoader().getResource("uniprot/PMID.GO.dr_ext.txt");
            assertNotNull(goTermsPath);
            URL flatFilePath = getClass().getClassLoader().getResource("uniprot/flatFIleSample.txt");
            assertNotNull(flatFilePath);
            URL disease = getClass().getClassLoader().getResource("uniprot/humdisease.txt");
            assertNotNull(disease);
            URL subcell = getClass().getClassLoader().getResource("uniprot/subcell.txt");
            assertNotNull(subcell);
            VoldemortUniprotEntryStoreBuilder.setFilePaths(disease.getPath(),keywordFile.getPath(),goTermsPath.getPath(),subcell.getPath());
            voldemortEntryStoreBuiler.retryBuild(flatFilePath.getPath());
            Map<String, Counter> statistics = voldemortEntryStoreBuiler.getExecutionStatistics();
            System.out.println("STATISTICS: "+statistics);
            assertNotNull(statistics);
            assertEquals(statistics.get(storeName+"-entry-parse-fail").getCount(),0);
            assertEquals(statistics.get(storeName+"-entry-parse-success").getCount(),6);
            assertEquals(statistics.get(storeName+"-entry-total").getCount(),6);
            assertEquals(statistics.get(storeName+"-entry-store-fail").getCount(),0);
            assertEquals(statistics.get(storeName+"-entry-store-success").getCount(),6);
            assertEquals(statistics.get(storeName+"-entry-store-success-isoform").getCount(),1);
            assertEquals(statistics.get(storeName+"-entry-store-success-swissprot").getCount(),1);
            assertEquals(statistics.get(storeName+"-entry-store-success-trembl").getCount(),4);
            assertEquals(statistics.get(storeName+"-store-try").getCount(),1);
        } catch (Exception e) {
            fail("Exception Running testBuild: "+e.getMessage());
        }
    }

    @Test
    public void testWithRetryVoldemortUniprotBuild(){
        FakeVoldemortInMemoryUniprotEntryStore voldemortInMemoryEntryStore = FakeVoldemortInMemoryUniprotEntryStore.getInstance(storeName);
        voldemortInMemoryEntryStore.setErrorFactor(2);// It will throws an exception after every 3 save request
        try{ ;
            URL keywordFile = VoldemortInMemoryUniprotEntryStoreTest.class.getResource("/uniprot/keywlist.txt");
            assertNotNull(keywordFile);
            VoldemortUniprotEntryStoreBuilder voldemortEntryStoreBuiler = new VoldemortUniprotEntryStoreBuilder(voldemortInMemoryEntryStore, storeName);
            URL goTermsPath = getClass().getClassLoader().getResource("uniprot/PMID.GO.dr_ext.txt");
            assertNotNull(goTermsPath);
            URL flatFilePath = getClass().getClassLoader().getResource("uniprot/flatFIleSample.txt");
            assertNotNull(flatFilePath);
            URL disease = getClass().getClassLoader().getResource("uniprot/humdisease.txt");
            assertNotNull(disease);
            URL subcell = getClass().getClassLoader().getResource("uniprot/subcell.txt");
            assertNotNull(subcell);
            VoldemortUniprotEntryStoreBuilder.setFilePaths(disease.getPath(),keywordFile.getPath(),goTermsPath.getPath(),subcell.getPath());
            voldemortEntryStoreBuiler.retryBuild(flatFilePath.getPath());
            Map<String, Counter> statistics = voldemortEntryStoreBuiler.getExecutionStatistics();
            assertNotNull(statistics);
            assertEquals(statistics.get(storeName+"-entry-parse-fail").getCount(),0);
            assertEquals(statistics.get(storeName+"-entry-parse-success").getCount(),11);
            assertEquals(statistics.get(storeName+"-entry-total").getCount(),6);
            assertEquals(statistics.get(storeName+"-entry-store-fail").getCount(),5);
            assertEquals(statistics.get(storeName+"-entry-store-success").getCount(),6);
            assertEquals(statistics.get(storeName+"-entry-store-success-isoform").getCount(),1);
            assertEquals(statistics.get(storeName+"-entry-store-success-swissprot").getCount(),1);
            assertEquals(statistics.get(storeName+"-entry-store-success-trembl").getCount(),4);
            assertEquals(statistics.get(storeName+"-store-try").getCount(),4);
        } catch (Exception e) {
            fail("Exception Running testBuild: "+e.getMessage());
        }
    }



}
package org.uniprot.store.datastore.voldemort.data.validator;

import com.codahale.metrics.Counter;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uniprot.core.flatfile.parser.UniProtParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.flatfile.parser.impl.EntryBufferedReader2;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.voldemort.data.validator.UniprotEntryRetrieveParseVerifier;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Map;

import static org.junit.Assert.*;

public class UniprotEntryRetrieveParseVerifierTest {


    private static String storeName = "avro-uniprot";
    private static VoldemortInMemoryUniprotEntryStore voldemortInMemoryEntryStore;

    @After
    public void cleanIndexFiles() throws Exception{
        new File("uniprot.parse.fail.txt").delete();
    }

    @BeforeClass
    public static void loadData() throws Exception{
        URL resourcePath = UniprotEntryRetrieveParseVerifierTest.class.getClassLoader().getResource("uniprot/flatFIleSample.txt");
        assert resourcePath != null;
        UniProtParser parser = new DefaultUniProtParser(new SupportingDataMapImpl("","","",""),true);

        Field instance = VoldemortInMemoryUniprotEntryStore.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
        voldemortInMemoryEntryStore = VoldemortInMemoryUniprotEntryStore.getInstance(storeName);

        EntryBufferedReader2 entryBufferReader2 = new EntryBufferedReader2(resourcePath.getPath());
        int size = 0;
        do{
            String next = null;
            try {
                next = entryBufferReader2.next();
            } catch (Exception e) {
                System.out.println("Finished to load "+resourcePath.getPath()+", with "+size+" entries");
            }

            if (next == null) {
                break;
            } else {
                UniProtEntry entry = parser.parse(next);
                voldemortInMemoryEntryStore.saveEntry(entry);
                size++;
            }
        }while (true);
    }

    @Test
    public void testSuccessVoldemortUniprotVerification(){
        try{
            UniprotEntryRetrieveParseVerifier dataVerification = new UniprotEntryRetrieveParseVerifier(voldemortInMemoryEntryStore);

            URL flatFilePath = getClass().getClassLoader().getResource("uniprot/flatFIleSample.txt");
            assertNotNull(flatFilePath);
            dataVerification.executeVerification(flatFilePath.getPath());

            Map<String, Counter> statistics = dataVerification.getExecutionStatistics();
            System.out.println("STATISTICS: "+statistics);
            assertNotNull(statistics);
            assertEquals(statistics.get("uniprot-entry-total").getCount(),6);
            assertEquals(statistics.get("uniprot-entry-parse-fail").getCount(),0);
            assertEquals(statistics.get("uniprot-entry-parse-success-isoform").getCount(),1);
            assertEquals(statistics.get("uniprot-entry-parse-success-swissprot").getCount(),1);
            assertEquals(statistics.get("uniprot-entry-parse-success-trembl").getCount(),4);
            assertEquals(statistics.get("uniprot-entry-not-found").getCount(),0);

        } catch (Exception e) {
            fail("Exception Running testBuild: "+e.getMessage());
        }
    }
}
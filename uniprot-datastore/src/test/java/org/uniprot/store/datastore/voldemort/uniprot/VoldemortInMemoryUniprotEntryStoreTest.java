package org.uniprot.store.datastore.voldemort.uniprot;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.UniProtParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.flatfile.parser.impl.EntryBufferedReader2;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class VoldemortInMemoryUniprotEntryStoreTest {

    private static List<String> savedAccessions;
    private static VoldemortInMemoryUniprotEntryStore voldemortInMemoryEntryStore;
    private static final String storeName = "avro-uniprot";

    @BeforeAll
    public static void loadData() throws Exception{
        URL resourcePath = VoldemortInMemoryUniprotEntryStoreTest.class.getClassLoader().getResource("uniprot/flatFIleSample.txt");
        assert resourcePath != null;
        URL keywordFile = VoldemortInMemoryUniprotEntryStoreTest.class.getResource("/uniprot/keywlist.txt");
        URL disease = VoldemortInMemoryUniprotEntryStoreTest.class.getClassLoader().getResource("uniprot/humdisease.txt");
        assertNotNull(disease);
        URL subcell = VoldemortInMemoryUniprotEntryStoreTest.class.getClassLoader().getResource("uniprot/subcell.txt");
        assertNotNull(subcell);
        UniProtParser parser = new DefaultUniProtParser(new SupportingDataMapImpl(keywordFile.getPath(),disease.getPath(),"",subcell.getPath()),false);

        Field instance = VoldemortInMemoryUniprotEntryStore.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
        voldemortInMemoryEntryStore = VoldemortInMemoryUniprotEntryStore.getInstance(storeName);

        savedAccessions = new ArrayList<>();
        EntryBufferedReader2 entryBufferReader2 = new EntryBufferedReader2(resourcePath.getPath());
        do{
            String next = null;
            try {
                next = entryBufferReader2.next();
            } catch (Exception e) {
                System.out.println("Finished to load "+resourcePath.getPath()+", with "+savedAccessions.size()+" entries");
            }

            if (next == null) {
                break;
            } else {
                UniProtEntry entry = parser.parse(next);
                voldemortInMemoryEntryStore.saveEntry(entry);
                savedAccessions.add(entry.getPrimaryAccession().getValue());
            }
        }while (true);
    }

    @Test
    public void testStoreName() throws Exception {
        assertThat(savedAccessions,notNullValue());
        assertThat(savedAccessions.size(),is(6));
        assertThat(voldemortInMemoryEntryStore.getStoreName(),notNullValue());
        assertThat(voldemortInMemoryEntryStore.getStoreName(),is(storeName));
    }

    @Test
    public void testSavedEntries() throws Exception {
        assertThat(savedAccessions,notNullValue());
        assertThat(savedAccessions.size(),is(6));
        savedAccessions.forEach(accession -> {
            Optional<UniProtEntry> entry = voldemortInMemoryEntryStore.getEntry(accession);
            UniProtEntry foundEntry = entry.orElseGet(null);
            assertThat(foundEntry, notNullValue());
            assertThat(foundEntry.getPrimaryAccession().getValue(),is(accession));
            assertThat(foundEntry.getAnnotationScore(),not(0));
        });
    }

    @Test
    public void testUpdateEntries() throws Exception {
        assertThat(savedAccessions,notNullValue());
        assertThat(savedAccessions.size(),is(6));

/*        savedAccessions.forEach(accession -> {
            Optional<UniProtEntry> entry = voldemortInMemoryEntryStore.getEntry(accession);
            UniProtEntry foundEntry = entry.orElseGet(null);
            assertThat(foundEntry, notNullValue());
            foundEntry.getEntryInfo().setName(foundEntry.getEntryInfo().getName()+ " UPDATED accession"+accession);
            voldemortInMemoryEntryStore.updateEntry(foundEntry);
        });


        savedAccessions.forEach(accession -> {
            Optional<EntryObject> entry = voldemortInMemoryEntryStore.getEntry(accession);
            EntryObject foundEntry = entry.orElseGet(null);
            assertThat(foundEntry, notNullValue());
            assertThat(foundEntry.getEntryInfo().getName().toString(),endsWith(" UPDATED accession"+accession));
        });*/
    }

}
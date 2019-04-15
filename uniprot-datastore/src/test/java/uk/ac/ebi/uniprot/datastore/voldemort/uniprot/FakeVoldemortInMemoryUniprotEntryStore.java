package uk.ac.ebi.uniprot.datastore.voldemort.uniprot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortInMemoryEntryStore;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import voldemort.VoldemortException;

import java.util.ArrayList;
import java.util.List;

public class FakeVoldemortInMemoryUniprotEntryStore extends VoldemortInMemoryEntryStore<UniProtEntry> {

    private static FakeVoldemortInMemoryUniprotEntryStore instance;
    private static final Logger logger = LoggerFactory.getLogger(VoldemortInMemoryEntryStore.class);
    private int errorFactor = 10;
    private int counter = 0;
    List<String> failedEntries = new ArrayList<String>();

    public static FakeVoldemortInMemoryUniprotEntryStore getInstance(String storeName){
        if(instance == null){
            instance = new FakeVoldemortInMemoryUniprotEntryStore(storeName);
        }
        return instance;
    }

    public void setErrorFactor(int errorFactor){
        this.errorFactor = errorFactor;
    }

    private FakeVoldemortInMemoryUniprotEntryStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniProtEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }


    @Override
    public void saveEntry(UniProtEntry entry) {
        counter++;
        if(counter % errorFactor == 0){
            failedEntries.add(getStoreId(entry));
            throw new VoldemortException("Fake error with entry accession: "+getStoreId(entry)+" at index "+counter);
        }else{
            super.saveEntry(entry);
        }
    }
}

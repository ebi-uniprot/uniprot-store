package uk.ac.ebi.uniprot.datastore.voldemort.client.impl;

import com.google.inject.name.Named;
import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.datastore.voldemort.client.ClientFactory;
import uk.ac.ebi.uniprot.datastore.voldemort.client.UniProtClient;
import uk.ac.ebi.uniprot.datastore.voldemort.uniprot.VoldemortRemoteUniprotEntryStore;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

public class DefaultClientFactory implements ClientFactory {
    private static final int DEFAULT_MAX_CONNECTION=20;
    private final VoldemortClient<UniProtEntry> voldemortStore;
    private static final String VOLDEMORT_STORE = "avro-uniprot";

    public DefaultClientFactory(@Named("VoldemortURL") String voldemortUrl) {
        this(voldemortUrl, DEFAULT_MAX_CONNECTION);
     
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn) {
    	this(voldemortUrl, numberOfConn, VOLDEMORT_STORE);
      
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn, String storeName) {
        VoldemortClient<UniProtEntry> store =null;
       try{
            store = new VoldemortRemoteUniprotEntryStore(numberOfConn, storeName, voldemortUrl);
       }catch(RuntimeException e){
           e.printStackTrace();
          
       }
       this.voldemortStore = store;
    }

    
    @Override
    public UniProtClient createUniProtClient() {
        if(voldemortStore ==null){
            throw new RuntimeException("Voldemort Store initialization failed.");
        }
        return new UniProtClientImpl(voldemortStore);
    }

    @Override
    public void close() {
    //    if(voldemortStore !=null)
   //         voldemortStore.close();

    }

}

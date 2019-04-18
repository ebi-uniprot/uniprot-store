package uk.ac.ebi.uniprot.datastore.voldemort.client;


import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;


public interface UniProtClient extends VoldemortClient<UniProtEntry> {
    
}

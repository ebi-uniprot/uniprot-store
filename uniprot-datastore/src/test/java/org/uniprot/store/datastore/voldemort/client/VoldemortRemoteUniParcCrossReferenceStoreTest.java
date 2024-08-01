package org.uniprot.store.datastore.voldemort.client;

import java.util.List;
import java.util.Optional;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.util.Pair;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;

public class VoldemortRemoteUniParcCrossReferenceStoreTest {

    public static void main(String[] args) {
        String voldemortUrl = "tcp://wp-np3-dc.ebi.ac.uk:8666";
        String storeName = "cross-reference";
        try (VoldemortRemoteUniParcCrossReferenceStore store =
                new VoldemortRemoteUniParcCrossReferenceStore(1, storeName, voldemortUrl)) {
            String key = "UPI0000000001-SWISSPROT-P07612-1";
            Optional<Pair<String, List<UniParcCrossReference>>> crossReference =
                    store.getEntry(key);

            if (crossReference.isPresent()) {
                System.out.println("Retrieved entry: " + crossReference);
            } else {
                System.out.println("Error - Entry not found for key: " + key);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

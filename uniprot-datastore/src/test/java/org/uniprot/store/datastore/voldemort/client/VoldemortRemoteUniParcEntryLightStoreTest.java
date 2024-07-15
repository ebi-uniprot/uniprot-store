package org.uniprot.store.datastore.voldemort.client;

import java.util.Optional;

import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.datastore.voldemort.light.uniparc.VoldemortRemoteUniParcEntryLightStore;

public class VoldemortRemoteUniParcEntryLightStoreTest {

    public static void main(String[] args) {
        String voldemortUrl = "tcp://wp-np3-dc.ebi.ac.uk:8666";
        String storeName = "uniparc-light";
        try (VoldemortRemoteUniParcEntryLightStore store =
                new VoldemortRemoteUniParcEntryLightStore(1, storeName, voldemortUrl)) {
            String key = "UPI0000000001";
            Optional<UniParcEntryLight> uniParcEntryLight = store.getEntry(key);

            if (uniParcEntryLight.isPresent()) {
                System.out.println("Retrieved entry: " + uniParcEntryLight);
            } else {
                System.out.println("Error - Entry not found for key: " + key);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

package org.uniprot.store.datastore.voldemort.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.uniprot.core.CrossReference;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.datastore.voldemort.light.uniparc.VoldemortRemoteUniParcEntryLightStore;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortRemoteUniParcCrossReferenceStore;

public class VoldemortRemoteUniParcCrossReferenceStoreTest {

    public static void main(String[] args) {
        String voldemortUrl = "tcp://wp-np3-dc.ebi.ac.uk:8666";

        String uniParcId = "UPI0000000001";
        try (VoldemortRemoteUniParcCrossReferenceStore store =
                        new VoldemortRemoteUniParcCrossReferenceStore(
                                1, "cross-reference", voldemortUrl);
                VoldemortRemoteUniParcEntryLightStore storeLight =
                        new VoldemortRemoteUniParcEntryLightStore(
                                1, "uniparc-light", voldemortUrl)) {
            Optional<UniParcEntryLight> entryLightOpt = storeLight.getEntry(uniParcId);
            if (entryLightOpt.isPresent()) {
                UniParcEntryLight entryLight = entryLightOpt.get();
                System.out.println(
                        entryLight.getUniParcId()
                                + " with "
                                + entryLight.getNumberOfUniParcCrossReferences()
                                + " xrefs");
                if (entryLight.getNumberOfUniParcCrossReferences() > 0) {
                    List<UniParcCrossReference> xrefs =
                            new ArrayList<>(entryLight.getNumberOfUniParcCrossReferences());
                    int numberOfPages = (entryLight.getNumberOfUniParcCrossReferences() / 1000) + 1;
                    for (int i = 0; i < numberOfPages; i++) {
                        String key = uniParcId + "_" + i;
                        Optional<UniParcCrossReferencePair> crossReference = store.getEntry(key);
                        if (crossReference.isPresent()) {
                            xrefs.addAll(crossReference.get().getValue());
                        } else {
                            System.out.println("Error - Entry not found for key: " + key);
                        }
                    }
                    xrefs.stream().map(CrossReference::getId).forEach(System.out::println);

                    System.out.println("NUMBER OF XREF FOUND: " + xrefs.size());
                }
            } else {
                System.out.println("Error - Entry light not found for uniParcId: " + uniParcId);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

package org.uniprot.store.spark.indexer.main.experimental;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniparc.crossref.VoldemortInMemoryUniParcCrossReferenceStore;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.CommonVariables;

class UniParcCrossReferenceValidatorTest {

    @Test
    void testUniParcCrossReferenceValidatorInvalidArgumentIndex() {
        assertThrows(
                IllegalArgumentException.class,
                () -> UniParcCrossReferenceValidator.main(new String[0]));
    }

    @Test
    void testUniParcCrossReferenceValidatorWillFailToConnectToVoldemort() {
        String[] args = {"2020_02", CommonVariables.SPARK_LOCAL_MASTER};
        assertThrows(
                IndexDataStoreException.class, () -> UniParcCrossReferenceValidator.main(args));
    }

    @Test
    void testUniParcCrossReferenceAllValid() {
        // TODO: Need to fix it
        InMemoryCheckVoldermortXref xrefCheck = new InMemoryCheckVoldermortXref(null);
        UniParcEntryLight entryToCheck =
                new UniParcEntryLightBuilder()
                        .uniParcId("UP0000000001")
                        .crossReferenceCount(20)
                        .build();

        assertDoesNotThrow(() -> xrefCheck.call(List.of(entryToCheck).iterator()));
    }

    @Test
    void testUniParcCrossReferenceWithInvalid() {
        // TODO: Need to fix it
        InMemoryCheckVoldermortXref xrefCheck = new InMemoryCheckVoldermortXref(null);
        UniParcEntryLight entryToCheck =
                new UniParcEntryLightBuilder()
                        .uniParcId("UP0000000001")
                        .crossReferenceCount(10)
                        .build();

        IndexDataStoreException error =
                assertThrows(
                        IndexDataStoreException.class,
                        () -> xrefCheck.call(List.of(entryToCheck).iterator()));
        assertEquals("Unable to find xrefIds: INVALID,INVALID2", error.getMessage());
    }

    private static List<String> saveXrefsInVoldemort(
            VoldemortClient<UniParcCrossReference> dataStore, int numberIds) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < numberIds; i++) {
            String id = "ID-" + i;
            UniParcCrossReference xrefEntry = new UniParcCrossReferenceBuilder().id(id).build();
            dataStore.saveEntry(xrefEntry);
            ids.add(id);
        }
        return ids;
    }

    private static class InMemoryCheckVoldermortXref
            extends UniParcCrossReferenceValidator.CheckVoldermortXref {

        public InMemoryCheckVoldermortXref(DataStoreParameter parameter) {
            super(parameter);
        }

        @Override
        protected VoldemortClient<UniParcCrossReferencePair> getDataStoreClient() {
            return VoldemortInMemoryUniParcCrossReferenceStore.getInstance("cross-reference");
        }
    }
}

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

    private static final int BATCH_SIZE = 3;
    private static final String UNIPARC_ID = "UPI000000012";

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
        int xrefCount = 20;
        InMemoryCheckVoldermortXref xrefCheck = new InMemoryCheckVoldermortXref(null, BATCH_SIZE);
        UniParcEntryLight entryToCheck =
                new UniParcEntryLightBuilder()
                        .uniParcId(UNIPARC_ID)
                        .crossReferenceCount(xrefCount)
                        .build();

        saveXrefsInVoldemort(xrefCheck.getDataStoreClient(), xrefCount);
        assertDoesNotThrow(() -> xrefCheck.call(List.of(entryToCheck).iterator()));
    }

    @Test
    void testUniParcCrossReferenceWithInvalidCount() {
        InMemoryCheckVoldermortXref xrefCheck = new InMemoryCheckVoldermortXref(null, BATCH_SIZE);
        UniParcEntryLight entryToCheck =
                new UniParcEntryLightBuilder()
                        .uniParcId(UNIPARC_ID)
                        .crossReferenceCount(10)
                        .build();

        saveXrefsInVoldemort(xrefCheck.getDataStoreClient(), 9);
        IndexDataStoreException error =
                assertThrows(
                        IndexDataStoreException.class,
                        () -> xrefCheck.call(List.of(entryToCheck).iterator()));
        assertEquals(
                "ISSUES FOUND: Unable to find Page: UPI000000012_3 AND Entry count mismatch for UniParcID: UPI000000012 entry count: 10 with total found in store 9 ",
                error.getMessage());
    }

    @Test
    void testUniParcCrossReferenceWithoutXref() {
        InMemoryCheckVoldermortXref xrefCheck = new InMemoryCheckVoldermortXref(null, BATCH_SIZE);
        UniParcEntryLight entryToCheck =
                new UniParcEntryLightBuilder().uniParcId(UNIPARC_ID).crossReferenceCount(0).build();

        IndexDataStoreException error =
                assertThrows(
                        IndexDataStoreException.class,
                        () -> xrefCheck.call(List.of(entryToCheck).iterator()));
        assertEquals(
                "ISSUES FOUND: UniParcID: UPI000000012 does not have Cross Reference",
                error.getMessage());
    }

    private static void saveXrefsInVoldemort(
            VoldemortClient<UniParcCrossReferencePair> dataStore, int numberIds) {
        List<UniParcCrossReference> pageXrefs = new ArrayList<>();
        int pageNumber = 0;
        for (int i = 0; i < numberIds; ) {
            String id = "ID-" + i;
            pageXrefs.add(new UniParcCrossReferenceBuilder().id(id).build());
            if (++i % BATCH_SIZE == 0) {
                dataStore.saveEntry(
                        new UniParcCrossReferencePair(UNIPARC_ID + "_" + pageNumber++, pageXrefs));
                pageXrefs = new ArrayList<>();
            }
        }
        if (!pageXrefs.isEmpty()) {
            dataStore.saveEntry(
                    new UniParcCrossReferencePair(UNIPARC_ID + "_" + pageNumber, pageXrefs));
        }
    }

    private static class InMemoryCheckVoldermortXref
            extends UniParcCrossReferenceValidator.CheckVoldermortXref {

        public InMemoryCheckVoldermortXref(DataStoreParameter parameter, int batchSize) {
            super(parameter, batchSize);
        }

        @Override
        protected VoldemortClient<UniParcCrossReferencePair> getDataStoreClient() {
            return VoldemortInMemoryUniParcCrossReferenceStore.getInstance("cross-reference");
        }
    }
}

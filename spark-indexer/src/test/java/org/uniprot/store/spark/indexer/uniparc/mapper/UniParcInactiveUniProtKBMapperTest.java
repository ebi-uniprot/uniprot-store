package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.uniparc.UniParcDatabase.*;

import java.util.*;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;

import scala.Tuple2;

class UniParcInactiveUniProtKBMapperTest {
    @Test
    void mapUniParcEntryInactiveUniProtKBCrossReferences() throws Exception {
        UniParcInactiveUniProtKBMapper mapper = new UniParcInactiveUniProtKBMapper();
        String uniParcId = "UPI0000000001";
        UniParcEntry uniParcEntry =
                new UniParcEntryBuilder()
                        .uniParcId(uniParcId)
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(EMBL, "ID1", true))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(EMBL, "ID2", false))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(TREMBL, "ID3", true))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(TREMBL, "ID4", false))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(SWISSPROT, "ID5", true))
                        .uniParcCrossReferencesAdd(
                                getUniParcCrossReference(SWISSPROT, "ID6", false))
                        .uniParcCrossReferencesAdd(
                                getUniParcCrossReference(SWISSPROT_VARSPLIC, "ID7", true))
                        .uniParcCrossReferencesAdd(
                                getUniParcCrossReference(SWISSPROT_VARSPLIC, "ID8", false))
                        .build();
        Iterator<Tuple2<String, String>> resultIterator = mapper.call(uniParcEntry);
        assertNotNull(resultIterator);
        Map<String, String> result = new HashMap<>();
        resultIterator.forEachRemaining(item -> result.put(item._1, item._2));
        assertEquals(3, result.size());
        result.values().forEach(value -> assertEquals(uniParcId, value));
        assertTrue(result.containsKey("ID4"));
        assertTrue(result.containsKey("ID6"));
        assertTrue(result.containsKey("ID8"));
    }

    @Test
    void mapUniParcEntryWithoutResult() throws Exception {
        UniParcInactiveUniProtKBMapper mapper = new UniParcInactiveUniProtKBMapper();
        String uniParcId = "UPI0000000001";
        UniParcEntry uniParcEntry =
                new UniParcEntryBuilder()
                        .uniParcId(uniParcId)
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(EMBL, "ID1", false))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(TREMBL, "ID2", true))
                        .uniParcCrossReferencesAdd(getUniParcCrossReference(SWISSPROT, "ID3", true))
                        .build();
        Iterator<Tuple2<String, String>> resultIterator = mapper.call(uniParcEntry);
        assertNotNull(resultIterator);
        assertFalse(resultIterator.hasNext());
    }

    @NotNull
    private static UniParcCrossReference getUniParcCrossReference(
            UniParcDatabase database, String id, boolean active) {
        return new UniParcCrossReferenceBuilder().database(database).id(id).active(active).build();
    }
}

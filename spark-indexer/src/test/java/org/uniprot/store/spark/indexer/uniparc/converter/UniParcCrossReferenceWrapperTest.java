package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;

class UniParcCrossReferenceWrapperTest {
    @Test
    void testCreateObject() {
        String xrefId = "UPI0001-EMBL-P12345";
        UniParcCrossReferenceBuilder builder = new UniParcCrossReferenceBuilder();
        UniParcCrossReference xref = builder.id("P12345").database(UniParcDatabase.EMBL).build();
        UniParcCrossReferenceWrapper xrefWrapper = new UniParcCrossReferenceWrapper(xrefId, xref);
        assertNotNull(xrefWrapper);
        assertEquals(xrefId, xrefWrapper.getId());
    }
}

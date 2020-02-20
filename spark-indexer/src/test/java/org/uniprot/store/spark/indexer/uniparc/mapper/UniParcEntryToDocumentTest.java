package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.builder.UniParcEntryBuilder;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-02-20
 */
class UniParcEntryToDocumentTest {

    @Test
    void testMapUniParcEntryToDocument() throws Exception {
        UniParcToDocument mapper = new UniParcToDocument();
        UniParcEntry entry =
                new UniParcEntryBuilder()
                        .uniParcId("uniParcIdValue")
                        .sequence(new SequenceImpl("MVSWGRFICLVVVTMATLSLAR"))
                        .build();
        Tuple2<String, UniParcDocument> result = mapper.call(entry);
        assertNotNull(result);
        assertNotNull(result._1);
        assertEquals("uniParcIdValue", result._1);

        assertNotNull(result._2);
        assertEquals("uniParcIdValue", result._2.getUpi());
    }
}

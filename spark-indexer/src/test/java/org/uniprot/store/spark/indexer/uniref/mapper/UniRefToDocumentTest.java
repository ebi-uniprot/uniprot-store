package org.uniprot.store.spark.indexer.uniref.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.core.uniref.builder.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.builder.UniRefEntryBuilder;
import org.uniprot.store.search.document.uniref.UniRefDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-02-10
 */
class UniRefToDocumentTest {

    @Test
    void testUniRefToDocument() throws Exception {
        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .organismTaxId(1)
                        .organismName("name")
                        .sequence(new SequenceImpl("AAAAA"))
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .representativeMember(representativeMember)
                        .entryType(UniRefType.UniRef50)
                        .id("id")
                        .name("name")
                        .updated(LocalDate.now())
                        .build();

        UniRefToDocument mapper = new UniRefToDocument();
        Tuple2<String, UniRefDocument> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("1", result._1);
        assertNotNull(result._2);
        assertEquals("id", result._2.getId());
    }
}

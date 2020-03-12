package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.core.uniprotkb.UniProtkbEntryType;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtkbEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtkbCrossReference;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;
import org.uniprot.cv.xdb.UniProtkbDatabaseImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GOEvidenceMapperTest {

    @Test
    void testMapGoEvidences() throws Exception {

        // given
        UniProtkbCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtkbCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtkbEntry entry =
                new UniProtkbEntryBuilder("P12345", "ID_P12345", UniProtkbEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Evidence evidence =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000256)
                        .databaseName("PubMed")
                        .databaseId("99999")
                        .build();
        GOEvidence goEvidence = new GOEvidence("GO:12345", evidence);
        List<GOEvidence> goEvidences = new ArrayList<>();
        goEvidences.add(goEvidence);

        Tuple2<UniProtkbEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(goEvidences));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtkbEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtkbCrossReferences().size());

        java.util.Optional<UniProtkbCrossReference> withEvidence =
                result.getUniProtkbCrossReferences().stream()
                        .filter(UniProtkbCrossReference::hasEvidences)
                        .findFirst();

        assertTrue(withEvidence.isPresent());
        UniProtkbCrossReference xrefWithEvidence = withEvidence.get();

        assertEquals("GO:12345", xrefWithEvidence.getId());
        assertEquals(1, xrefWithEvidence.getEvidences().size());
        assertEquals(evidence, xrefWithEvidence.getEvidences().get(0));

        java.util.Optional<UniProtkbCrossReference> withoutEvidence =
                result.getUniProtkbCrossReferences().stream()
                        .filter(xref -> !xref.hasEvidences())
                        .findFirst();

        assertTrue(withoutEvidence.isPresent());
        UniProtkbCrossReference xrefWithoutEvidence = withoutEvidence.get();
        assertEquals("GO:11111", xrefWithoutEvidence.getId());
        assertEquals(0, xrefWithoutEvidence.getEvidences().size());
    }

    @Test
    void testWithoutGoEvidencesEmptyList() throws Exception {

        // given
        UniProtkbCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtkbCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("PDB"))
                        .id("PDB11111")
                        .build();

        UniProtkbEntry entry =
                new UniProtkbEntryBuilder("P12345", "ID_P12345", UniProtkbEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtkbEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(new ArrayList<>()));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtkbEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtkbCrossReferences().size());

        java.util.Optional<UniProtkbCrossReference> withEvidence =
                result.getUniProtkbCrossReferences().stream()
                        .filter(UniProtkbCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }

    @Test
    void testWithoutGoEvidencesEmptyOptional() throws Exception {

        // given
        UniProtkbCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtkbCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtkbDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtkbEntry entry =
                new UniProtkbEntryBuilder("P12345", "ID_P12345", UniProtkbEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtkbEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.empty());

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtkbEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtkbCrossReferences().size());

        java.util.Optional<UniProtkbCrossReference> withEvidence =
                result.getUniProtkbCrossReferences().stream()
                        .filter(UniProtkbCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }
}

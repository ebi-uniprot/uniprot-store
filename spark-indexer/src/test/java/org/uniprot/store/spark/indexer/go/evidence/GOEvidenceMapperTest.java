package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;
import org.uniprot.cv.xdb.UniProtKBDatabaseImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GOEvidenceMapperTest {

    @Test
    void testMapGoEvidences() throws Exception {

        // given
        UniProtKBCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtKBCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
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

        Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(goEvidences));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtKBEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtKBCrossReferences().size());

        java.util.Optional<UniProtKBCrossReference> withEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(UniProtKBCrossReference::hasEvidences)
                        .findFirst();

        assertTrue(withEvidence.isPresent());
        UniProtKBCrossReference xrefWithEvidence = withEvidence.get();

        assertEquals("GO:12345", xrefWithEvidence.getId());
        assertEquals(1, xrefWithEvidence.getEvidences().size());
        assertEquals(evidence, xrefWithEvidence.getEvidences().get(0));

        java.util.Optional<UniProtKBCrossReference> withoutEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(xref -> !xref.hasEvidences())
                        .findFirst();

        assertTrue(withoutEvidence.isPresent());
        UniProtKBCrossReference xrefWithoutEvidence = withoutEvidence.get();
        assertEquals("GO:11111", xrefWithoutEvidence.getId());
        assertEquals(0, xrefWithoutEvidence.getEvidences().size());
    }

    @Test
    void testMapGoEvidencesWithDuplicateGoEvidences() throws Exception {

        // given
        UniProtKBCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtKBCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
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

        Evidence evidenceDuplicated =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000256)
                        .databaseName("PubMed")
                        .databaseId("99999")
                        .build();
        GOEvidence goEvidenceDuplicated = new GOEvidence("GO:12345", evidenceDuplicated);

        List<GOEvidence> goEvidences = List.of(goEvidence, goEvidenceDuplicated);

        Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(goEvidences));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtKBEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtKBCrossReferences().size());

        java.util.Optional<UniProtKBCrossReference> withEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(UniProtKBCrossReference::hasEvidences)
                        .findFirst();

        assertTrue(withEvidence.isPresent());
        UniProtKBCrossReference xrefWithEvidence = withEvidence.get();

        assertEquals("GO:12345", xrefWithEvidence.getId());
        assertEquals(1, xrefWithEvidence.getEvidences().size());
        assertEquals(evidence, xrefWithEvidence.getEvidences().get(0));

        java.util.Optional<UniProtKBCrossReference> withoutEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(xref -> !xref.hasEvidences())
                        .findFirst();

        assertTrue(withoutEvidence.isPresent());
        UniProtKBCrossReference xrefWithoutEvidence = withoutEvidence.get();
        assertEquals("GO:11111", xrefWithoutEvidence.getId());
        assertEquals(0, xrefWithoutEvidence.getEvidences().size());
    }

    @Test
    void testWithoutGoEvidencesEmptyList() throws Exception {

        // given
        UniProtKBCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtKBCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("PDB"))
                        .id("PDB11111")
                        .build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(new ArrayList<>()));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtKBEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtKBCrossReferences().size());

        java.util.Optional<UniProtKBCrossReference> withEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(UniProtKBCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }

    @Test
    void testWithoutGoEvidencesEmptyOptional() throws Exception {

        // given
        UniProtKBCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtKBCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtKBDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtKBEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.empty());

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtKBEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtKBCrossReferences().size());

        java.util.Optional<UniProtKBCrossReference> withEvidence =
                result.getUniProtKBCrossReferences().stream()
                        .filter(UniProtKBCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }
}

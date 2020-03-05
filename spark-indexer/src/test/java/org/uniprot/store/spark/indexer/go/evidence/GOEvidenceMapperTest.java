package org.uniprot.store.spark.indexer.go.evidence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.UniProtEntryType;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.core.uniprot.xdb.UniProtCrossReference;
import org.uniprot.core.uniprot.xdb.builder.UniProtCrossReferenceBuilder;
import org.uniprot.cv.xdb.UniProtDatabaseImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GOEvidenceMapperTest {

    @Test
    void testMapGoEvidences() throws Exception {

        // given
        UniProtCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", "ID_P12345", UniProtEntryType.SWISSPROT)
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

        Tuple2<UniProtEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(goEvidences));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtCrossReferences().size());

        java.util.Optional<UniProtCrossReference> withEvidence =
                result.getUniProtCrossReferences().stream()
                        .filter(UniProtCrossReference::hasEvidences)
                        .findFirst();

        assertTrue(withEvidence.isPresent());
        UniProtCrossReference xrefWithEvidence = withEvidence.get();

        assertEquals("GO:12345", xrefWithEvidence.getId());
        assertEquals(1, xrefWithEvidence.getEvidences().size());
        assertEquals(evidence, xrefWithEvidence.getEvidences().get(0));

        java.util.Optional<UniProtCrossReference> withoutEvidence =
                result.getUniProtCrossReferences().stream()
                        .filter(xref -> !xref.hasEvidences())
                        .findFirst();

        assertTrue(withoutEvidence.isPresent());
        UniProtCrossReference xrefWithoutEvidence = withoutEvidence.get();
        assertEquals("GO:11111", xrefWithoutEvidence.getId());
        assertEquals(0, xrefWithoutEvidence.getEvidences().size());
    }

    @Test
    void testWithoutGoEvidencesEmptyList() throws Exception {

        // given
        UniProtCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("PDB"))
                        .id("PDB11111")
                        .build();

        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", "ID_P12345", UniProtEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.of(new ArrayList<>()));

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtCrossReferences().size());

        java.util.Optional<UniProtCrossReference> withEvidence =
                result.getUniProtCrossReferences().stream()
                        .filter(UniProtCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }

    @Test
    void testWithoutGoEvidencesEmptyOptional() throws Exception {

        // given
        UniProtCrossReference goCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("GO"))
                        .id("GO:12345")
                        .build();

        UniProtCrossReference otherGoCrossReference =
                new UniProtCrossReferenceBuilder()
                        .database(new UniProtDatabaseImpl("GO"))
                        .id("GO:11111")
                        .build();

        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", "ID_P12345", UniProtEntryType.SWISSPROT)
                        .uniProtCrossReferencesAdd(goCrossReference)
                        .uniProtCrossReferencesAdd(otherGoCrossReference)
                        .build();

        Tuple2<UniProtEntry, Optional<Iterable<GOEvidence>>> tuple =
                new Tuple2<>(entry, Optional.empty());

        // when
        GOEvidenceMapper mapper = new GOEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getUniProtCrossReferences().size());

        java.util.Optional<UniProtCrossReference> withEvidence =
                result.getUniProtCrossReferences().stream()
                        .filter(UniProtCrossReference::hasEvidences)
                        .findFirst();

        assertFalse(withEvidence.isPresent());
    }
}

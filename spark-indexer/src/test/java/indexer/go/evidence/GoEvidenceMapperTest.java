package indexer.go.evidence;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.core.uniprot.xdb.UniProtDBCrossReference;
import org.uniprot.core.uniprot.xdb.UniProtXDbType;
import org.uniprot.core.uniprot.xdb.builder.UniProtDBCrossReferenceBuilder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class GoEvidenceMapperTest {

    @Test
    void testMapGoEvidences() throws Exception{

        // given
        UniProtDBCrossReference goCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("GO"))
                .id("GO:12345")
                .build();

        UniProtDBCrossReference otherGoCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("GO"))
                .id("GO:11111")
                .build();

        UniProtEntry entry = new UniProtEntryBuilder()
                .primaryAccession(null)
                .uniProtId(null)
                .active()
                .addDatabaseCrossReference(goCrossReference)
                .addDatabaseCrossReference(otherGoCrossReference)
                .build();

        Evidence evidence = new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000256)
                .databaseName("PubMed")
                .databaseId("99999")
                .build();
        GoEvidence goEvidence = new GoEvidence("GO:12345", evidence);
        List<GoEvidence> goEvidences = new ArrayList<>();
        goEvidences.add(goEvidence);

        Tuple2<UniProtEntry, Optional<Iterable<GoEvidence>>> tuple = new Tuple2<>(entry,Optional.of(goEvidences));

        // when
        GoEvidenceMapper mapper = new GoEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getDatabaseCrossReferences().size());

        java.util.Optional<UniProtDBCrossReference> withEvidence = result.getDatabaseCrossReferences()
                .stream()
                .filter(UniProtDBCrossReference::hasEvidences)
                .findFirst();

        assertTrue(withEvidence.isPresent());
        UniProtDBCrossReference xrefWithEvidence = withEvidence.get();

        assertEquals("GO:12345", xrefWithEvidence.getId());
        assertEquals(1, xrefWithEvidence.getEvidences().size());
        assertEquals(evidence, xrefWithEvidence.getEvidences().get(0));

        java.util.Optional<UniProtDBCrossReference> withoutEvidence = result.getDatabaseCrossReferences()
                .stream()
                .filter(xref -> !xref.hasEvidences())
                .findFirst();

        assertTrue(withoutEvidence.isPresent());
        UniProtDBCrossReference xrefWithoutEvidence = withoutEvidence.get();
        assertEquals("GO:11111", xrefWithoutEvidence.getId());
        assertEquals(0, xrefWithoutEvidence.getEvidences().size());
    }


    @Test
    void testWithoutGoEvidencesEmptyList() throws Exception{

        // given
        UniProtDBCrossReference goCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("GO"))
                .id("GO:12345")
                .build();

        UniProtDBCrossReference otherGoCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("PDB"))
                .id("PDB11111")
                .build();

        UniProtEntry entry = new UniProtEntryBuilder()
                .primaryAccession(null)
                .uniProtId(null)
                .active()
                .addDatabaseCrossReference(goCrossReference)
                .addDatabaseCrossReference(otherGoCrossReference)
                .build();

        Tuple2<UniProtEntry, Optional<Iterable<GoEvidence>>> tuple = new Tuple2<>(entry,Optional.of(new ArrayList<>()));

        // when
        GoEvidenceMapper mapper = new GoEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getDatabaseCrossReferences().size());

        java.util.Optional<UniProtDBCrossReference> withEvidence = result.getDatabaseCrossReferences()
                .stream()
                .filter(UniProtDBCrossReference::hasEvidences)
                .findFirst();

        assertFalse(withEvidence.isPresent());
    }

    @Test
    void testWithoutGoEvidencesEmptyOptional() throws Exception{

        // given
        UniProtDBCrossReference goCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("GO"))
                .id("GO:12345")
                .build();

        UniProtDBCrossReference otherGoCrossReference = new UniProtDBCrossReferenceBuilder()
                .databaseType(new UniProtXDbType("GO"))
                .id("GO:11111")
                .build();

        UniProtEntry entry = new UniProtEntryBuilder()
                .primaryAccession(null)
                .uniProtId(null)
                .active()
                .addDatabaseCrossReference(goCrossReference)
                .addDatabaseCrossReference(otherGoCrossReference)
                .build();

        Tuple2<UniProtEntry, Optional<Iterable<GoEvidence>>> tuple = new Tuple2<>(entry,Optional.empty());

        // when
        GoEvidenceMapper mapper = new GoEvidenceMapper();
        UniProtEntry result = mapper.call(tuple);

        // then
        assertNotNull(result);
        assertEquals(2, result.getDatabaseCrossReferences().size());

        java.util.Optional<UniProtDBCrossReference> withEvidence = result.getDatabaseCrossReferences()
                .stream()
                .filter(UniProtDBCrossReference::hasEvidences)
                .findFirst();

        assertFalse(withEvidence.isPresent());
    }
}
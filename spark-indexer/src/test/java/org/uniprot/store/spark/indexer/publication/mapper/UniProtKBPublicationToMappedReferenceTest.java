package org.uniprot.store.spark.indexer.publication.mapper;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.JournalArticleBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBReferenceBuilder;

import scala.Tuple2;

class UniProtKBPublicationToMappedReferenceTest {
    @Test
    void loadsCorrectlyHumanMultipleReferences() throws Exception {
        UniProtKBPublicationToMappedReference converter =
                new UniProtKBPublicationToMappedReference();
        String flatFile = "2020_02/uniprotkb/O60260.txt";
        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Iterator<Tuple2<String, MappedReference>> result =
                converter.call(String.join("\n", flatFileLines));

        assertNotNull(result);
        assertTrue(result.hasNext());
        int counter = 0;
        Tuple2<String, MappedReference> firstTuple = null;
        Tuple2<String, MappedReference> fourthTuple = null;
        Tuple2<String, MappedReference> lastTuple = null;
        Tuple2<String, MappedReference> currentTuple = null;
        boolean first = true;
        while (result.hasNext()) {
            Tuple2<String, MappedReference> tuple = result.next();
            currentTuple = tuple;
            if (first) {
                firstTuple = tuple;
                first = false;
            }

            if (counter == 3) {
                fourthTuple = currentTuple;
            }
            counter++;
        }
        lastTuple = currentTuple;

        assertThat(counter, is(84));
        // RN 1
        assertThat(firstTuple, is(notNullValue()));
        assertThat(firstTuple._1, is("O60260_9560156"));
        assertThat(firstTuple._2, is(notNullValue()));
        MappedReference mappedReference = firstTuple._2;
        assertThat(mappedReference.getSourceCategories(), hasItem("Disease & Variants"));

        // RN 4
        assertThat(fourthTuple, is(notNullValue()));
        assertThat(fourthTuple._1, is("O60260_CI-36POVM3NV8D55"));

        // RN 84
        assertThat(lastTuple, is(notNullValue()));
        assertThat(lastTuple._1, is("O60260_29311685"));
    }

    @Test
    void loadsCorrectlyNotHumanMultipleReferences() throws Exception {
        UniProtKBPublicationToMappedReference converter =
                new UniProtKBPublicationToMappedReference();
        String flatFile = "2020_02/uniprotkb/Q9JJ00.txt";
        List<String> flatFileLines =
                Files.readAllLines(Paths.get(ClassLoader.getSystemResource(flatFile).toURI()));
        Iterator<Tuple2<String, MappedReference>> result =
                converter.call(String.join("\n", flatFileLines));

        assertNotNull(result);
        assertTrue(result.hasNext());
        int counter = 0;
        Tuple2<String, MappedReference> firstTuple = null;
        Tuple2<String, MappedReference> fourthTuple = null;
        Tuple2<String, MappedReference> lastTuple = null;
        Tuple2<String, MappedReference> currentTuple = null;
        boolean first = true;
        while (result.hasNext()) {
            Tuple2<String, MappedReference> tuple = result.next();
            currentTuple = tuple;
            if (first) {
                firstTuple = tuple;
                first = false;
            }

            if (counter == 3) {
                fourthTuple = currentTuple;
            }
            counter++;
        }

        assertThat(counter, is(7));
        // RN 1
        assertThat(firstTuple, is(notNullValue()));
        assertThat(firstTuple._1, is("Q9JJ00_10930526"));

        // RN 4
        assertThat(fourthTuple, is(notNullValue()));
        assertThat(fourthTuple._1, is("Q9JJ00_12010804"));
        assertThat(fourthTuple._2, is(notNullValue()));
        MappedReference mappedReference = fourthTuple._2;
        assertThat(mappedReference.getSourceCategories(), hasItem("Phenotypes & Variants"));
    }

    @Test
    void createMappedReferenceInfoSkipsEvidenceWithoutCrossReference() {
        UniProtKBPublicationToMappedReference converter =
                new UniProtKBPublicationToMappedReference();
        Evidence evidence = new EvidenceBuilder().evidenceCode(EvidenceCode.ECO_0000269).build();
        CrossReference<CitationDatabase> pubmed =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
                        .id("12345")
                        .build();
        Citation citation =
                new JournalArticleBuilder()
                        .title("Title")
                        .citationCrossReferencesAdd(pubmed)
                        .build();
        UniProtKBReference reference =
                new UniProtKBReferenceBuilder().citation(citation).evidencesAdd(evidence).build();

        List<UniProtKBPublicationToMappedReference.MappedReferenceInfo> result =
                converter.createMappedReferenceInfo(
                        "P12345", UniProtKBEntryType.SWISSPROT, reference, 9606L, 0);

        assertEquals(1, result.size());
        assertEquals("12345", result.get(0).citationId);
    }
}

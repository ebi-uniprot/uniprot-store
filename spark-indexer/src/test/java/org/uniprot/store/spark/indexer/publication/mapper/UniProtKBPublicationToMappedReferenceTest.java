package org.uniprot.store.spark.indexer.publication.mapper;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.getJoinKey;
import static org.uniprot.store.spark.indexer.publication.PublicationDocumentsToHDFSWriter.separateJoinKey;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.MappedReference;

import scala.Tuple2;

class UniProtKBPublicationToMappedReferenceTest {
    @Test
    void loadsCorrectlyMultipleReferences() throws Exception {
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
        assertThat(firstTuple._1, is(getJoinKey("O60260", "9560156")));

        // RN 4
        assertThat(fourthTuple, is(notNullValue()));
        String[] fourthTupleKeyParts = separateJoinKey(fourthTuple._1);
        assertThat(fourthTupleKeyParts[0], is("O60260"));
        assertThat(fourthTupleKeyParts[1], is(nullValue()));

        // RN 84
        assertThat(lastTuple, is(notNullValue()));
        assertThat(lastTuple._1, is(getJoinKey("O60260", "29311685")));
    }
}

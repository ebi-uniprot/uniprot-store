package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

class ChebiToUniProtDocumentTest {

    @Test
    void canMapChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<String> catalyticIds = new ArrayList<>();
        catalyticIds.add("CHEBI:1");
        String catalyticKey =
                "cc_" + CommentType.CATALYTIC_ACTIVITY.name().toLowerCase(Locale.ROOT);
        doc.commentMap.put(catalyticKey, catalyticIds);
        doc.cofactorChebi.add("CHEBI:2");
        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder().id("1").inchiKey("inch1").relatedIdsAdd(relatedId1).build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .build();
        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));
        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));

        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));

        Collection<String> resultCatalytic = result.commentMap.get(catalyticKey);
        assertNotNull(resultCatalytic);
        assertTrue(resultCatalytic.contains("CHEBI:1"));
        assertTrue(resultCatalytic.contains("CHEBI:11"));

        assertTrue(result.cofactorChebi.contains("CHEBI:2"));
        assertTrue(result.cofactorChebi.contains("CHEBI:21"));
        assertTrue(result.cofactorChebi.contains("CHEBI:22"));
    }
}

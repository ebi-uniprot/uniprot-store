package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.KeywordBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

class GoogleProtNLMEntryUpdaterTest {

    private final GoogleProtNLMEntryUpdater updater = new GoogleProtNLMEntryUpdater(null);

    @Test
    void testUpdatesUniProtIdFromUniProtEntry() {
        // given
        String accession = "P12345";
        UniProtKBEntry protNLMEntry =
                new UniProtKBEntryBuilder(accession, "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .build();

        UniProtKBEntry uniProtEntry =
                new UniProtKBEntryBuilder(accession, "New Value", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(protNLMEntry, uniProtEntry);

        // when
        UniProtKBEntry updatedEntry = updater.call(input);

        // then
        assertEquals("New Value", updatedEntry.getUniProtkbId().getValue());
    }

    @Test
    void testWhenIdIsSameItStillBuildsNewEntry() {
        UniProtKBEntry original =
                new UniProtKBEntryBuilder("P12345", "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(original, original);

        UniProtKBEntry updated = updater.call(input);

        assertEquals("PLACEHOLDER", updated.getUniProtkbId().getValue());
        assertNotSame(original, updated); // ensure it's a new object
    }

    @Test
    void testUpdateKeywordCategory() {
        // given
        KeywordEntryBuilder keywordEntryBuilder = new KeywordEntryBuilder();
        String kwAccession = "KW-0001";
        String kwName = "2Fe-2S";
        KeywordEntry keywordEntry =
                keywordEntryBuilder
                        .keyword(new KeywordBuilder().id(kwAccession).name(kwName).build())
                        .category(KeywordCategory.LIGAND)
                        .build();
        Map<String, KeywordEntry> keywordAccEntryMap = Map.of(kwAccession, keywordEntry);
        String accession = "P12345";
        Keyword protnlmKeyword = new KeywordBuilder().id(kwAccession).name("2Fe-2S").build();
        UniProtKBEntry protNLMEntry =
                new UniProtKBEntryBuilder(accession, "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .keywordsAdd(protnlmKeyword)
                        .build();

        UniProtKBEntry uniProtEntry =
                new UniProtKBEntryBuilder(accession, "New Value", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(protNLMEntry, uniProtEntry);
        // when
        GoogleProtNLMEntryUpdater googleProtNLMEntryUpdater =
                new GoogleProtNLMEntryUpdater(keywordAccEntryMap);
        UniProtKBEntry updatedProtNLMEntry = googleProtNLMEntryUpdater.call(input);
        // then
        assertEquals("New Value", updatedProtNLMEntry.getUniProtkbId().getValue());
        assertEquals(1, updatedProtNLMEntry.getKeywords().size());
        assertEquals(kwAccession, updatedProtNLMEntry.getKeywords().get(0).getId());
        assertEquals("2Fe-2S", updatedProtNLMEntry.getKeywords().get(0).getName());
        assertEquals(
                KeywordCategory.LIGAND, updatedProtNLMEntry.getKeywords().get(0).getCategory());
    }
}

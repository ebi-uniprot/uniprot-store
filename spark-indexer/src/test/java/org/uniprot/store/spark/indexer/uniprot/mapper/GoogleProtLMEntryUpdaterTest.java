package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.uniprotkb.Keyword;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.SubcellularLocation;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationComment;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationValue;
import org.uniprot.core.uniprotkb.comment.impl.*;
import org.uniprot.core.uniprotkb.impl.KeywordBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

class GoogleProtNLMEntryUpdaterTest {

    private final GoogleProtNLMEntryUpdater updater = new GoogleProtNLMEntryUpdater(null, null);
    private final String accession = "P12345";
    private UniProtKBEntry uniProtEntry =
            new UniProtKBEntryBuilder(accession, "New Value", UniProtKBEntryType.TREMBL).build();
    ;

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
        Keyword protnlmKeyword = new KeywordBuilder().id(kwAccession).name("2Fe-2S").build();
        UniProtKBEntry protNLMEntry =
                new UniProtKBEntryBuilder(accession, "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .keywordsAdd(protnlmKeyword)
                        .build();

        this.uniProtEntry =
                new UniProtKBEntryBuilder(accession, "New Value", UniProtKBEntryType.TREMBL)
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(protNLMEntry, uniProtEntry);
        // when
        GoogleProtNLMEntryUpdater googleProtNLMEntryUpdater =
                new GoogleProtNLMEntryUpdater(keywordAccEntryMap, null);
        UniProtKBEntry updatedProtNLMEntry = googleProtNLMEntryUpdater.call(input);
        // then
        assertEquals("New Value", updatedProtNLMEntry.getUniProtkbId().getValue());
        assertEquals(1, updatedProtNLMEntry.getKeywords().size());
        assertEquals(kwAccession, updatedProtNLMEntry.getKeywords().get(0).getId());
        assertEquals("2Fe-2S", updatedProtNLMEntry.getKeywords().get(0).getName());
        assertEquals(
                KeywordCategory.LIGAND, updatedProtNLMEntry.getKeywords().get(0).getCategory());
    }

    @Test
    void testAddSubcellIds() {
        // given
        String subcellName0 = "Subcell0";
        SubcellularLocationValueBuilder subcellValueBuilder0 =
                new SubcellularLocationValueBuilder();
        SubcellularLocationValue subcellValue0 = subcellValueBuilder0.value(subcellName0).build();
        SubcellularLocationBuilder subcellBuilder0 = new SubcellularLocationBuilder();
        SubcellularLocation subcellLocation0 = subcellBuilder0.location(subcellValue0).build();

        String subcellName1 = "Subcell1";
        SubcellularLocationValueBuilder subcellValueBuilder1 =
                new SubcellularLocationValueBuilder();
        SubcellularLocationValue subcellValue1 = subcellValueBuilder1.value(subcellName1).build();
        SubcellularLocationBuilder subcellBuilder1 = new SubcellularLocationBuilder();
        SubcellularLocation subcellLocation1 = subcellBuilder1.location(subcellValue1).build();

        SubcellularLocationCommentBuilder subcellCommentBuilder =
                new SubcellularLocationCommentBuilder();
        subcellCommentBuilder.subcellularLocationsSet(List.of(subcellLocation0, subcellLocation1));
        SubcellularLocationComment subcellComment = subcellCommentBuilder.build();

        UniProtKBEntry protNLMEntry =
                new UniProtKBEntryBuilder("P12345", "PLACEHOLDER", UniProtKBEntryType.TREMBL)
                        .commentsAdd(subcellComment)
                        .commentsAdd(new BPCPCommentBuilder().build())
                        .build();

        Tuple2<UniProtKBEntry, UniProtKBEntry> input = new Tuple2<>(protNLMEntry, uniProtEntry);
        String subcellId0 = "SL-1234";
        String subcellId1 = "SL-5678";
        String subcellId2 = "SL-2222";
        String subcellName2 = "Subcell2";

        Map<String, SubcellularLocationEntry> subcellMap =
                Map.of(
                        subcellName0,
                        new SubcellularLocationEntryBuilder().id(subcellId0).build(),
                        subcellName1,
                        new SubcellularLocationEntryBuilder().id(subcellId1).build(),
                        subcellName2,
                        new SubcellularLocationEntryBuilder().id(subcellId2).build());
        GoogleProtNLMEntryUpdater googleProtNLMEntryUpdater =
                new GoogleProtNLMEntryUpdater(null, subcellMap);
        // when
        UniProtKBEntry updatedProtNLMEntry = googleProtNLMEntryUpdater.call(input);

        // then
        assertEquals("New Value", updatedProtNLMEntry.getUniProtkbId().getValue());
        assertEquals(2, updatedProtNLMEntry.getComments().size());
        SubcellularLocationComment enrichedSubcellComment =
                (SubcellularLocationComment) updatedProtNLMEntry.getComments().get(0);
        List<SubcellularLocation> enrichedSubcellLocations =
                enrichedSubcellComment.getSubcellularLocations();
        assertEquals(subcellId0, enrichedSubcellLocations.get(0).getLocation().getId());
        assertEquals(subcellId1, enrichedSubcellLocations.get(1).getLocation().getId());
    }
}

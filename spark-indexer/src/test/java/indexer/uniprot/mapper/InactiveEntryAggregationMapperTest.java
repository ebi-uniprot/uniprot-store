package indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprot.EntryInactiveReason;
import org.uniprot.core.uniprot.InactiveReasonType;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class InactiveEntryAggregationMapperTest {

    @Test
    void testEntryWithDemerged() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder().addMergeDemergeTo("P88888").build();
        UniProtEntry entry1 = new UniProtEntryBuilder("P99999", "ID_99999", reason1).build();

        EntryInactiveReason reason2 =
                new EntryInactiveReasonBuilder().addMergeDemergeTo("P77777").build();
        UniProtEntry entry2 = new UniProtEntryBuilder("P99999", "ID_99999", reason2).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtEntry result = mapper.call(entry1, entry2);

        assertNotNull(result);
        assertNotNull(result.getPrimaryAccession());
        assertEquals("P99999", result.getPrimaryAccession().getValue());
        assertNotNull(result.getUniProtId());
        assertEquals("ID_99999", result.getUniProtId().getValue());

        EntryInactiveReason inactiveReason = result.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.DEMERGED, inactiveReason.getInactiveReasonType());

        assertNotNull(inactiveReason.getMergeDemergeTo());
        assertEquals(2, inactiveReason.getMergeDemergeTo().size());
        assertEquals("P88888", inactiveReason.getMergeDemergeTo().get(0));
        assertEquals("P77777", inactiveReason.getMergeDemergeTo().get(1));
    }

    @Test
    void testEntryWithMerged() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.MERGED)
                        .addMergeDemergeTo("P88888")
                        .build();
        UniProtEntry entry1 = new UniProtEntryBuilder("P99999", "ID_99999", reason1).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtEntry result = mapper.call(entry1, null);

        assertNotNull(result);
        assertEquals(entry1, result);
    }

    @Test
    void testEntryWithDeleted() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.DELETED)
                        .addMergeDemergeTo("P88888")
                        .build();
        UniProtEntry entry1 = new UniProtEntryBuilder("P99999", "ID_99999", reason1).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtEntry result = mapper.call(entry1, null);

        assertNotNull(result);
        assertEquals(entry1, result);
    }
}

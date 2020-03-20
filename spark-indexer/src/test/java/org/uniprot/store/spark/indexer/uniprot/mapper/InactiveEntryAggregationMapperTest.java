package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class InactiveEntryAggregationMapperTest {

    @Test
    void testEntryWithDemerged() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder().mergeDemergeTosAdd("P88888").build();
        UniProtKBEntry entry1 = new UniProtKBEntryBuilder("P99999", "ID_99999", reason1).build();

        EntryInactiveReason reason2 =
                new EntryInactiveReasonBuilder().mergeDemergeTosAdd("P77777").build();
        UniProtKBEntry entry2 = new UniProtKBEntryBuilder("P99999", "ID_99999", reason2).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtKBEntry result = mapper.call(entry1, entry2);

        assertNotNull(result);
        assertNotNull(result.getPrimaryAccession());
        assertEquals("P99999", result.getPrimaryAccession().getValue());
        assertNotNull(result.getUniProtkbId());
        assertEquals("ID_99999", result.getUniProtkbId().getValue());

        EntryInactiveReason inactiveReason = result.getInactiveReason();
        assertNotNull(inactiveReason);

        assertNotNull(inactiveReason.getInactiveReasonType());
        assertEquals(InactiveReasonType.DEMERGED, inactiveReason.getInactiveReasonType());

        assertNotNull(inactiveReason.getMergeDemergeTos());
        assertEquals(2, inactiveReason.getMergeDemergeTos().size());
        assertEquals("P88888", inactiveReason.getMergeDemergeTos().get(0));
        assertEquals("P77777", inactiveReason.getMergeDemergeTos().get(1));
    }

    @Test
    void testEntryWithMerged() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.MERGED)
                        .mergeDemergeTosAdd("P88888")
                        .build();
        UniProtKBEntry entry1 = new UniProtKBEntryBuilder("P99999", "ID_99999", reason1).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtKBEntry result = mapper.call(entry1, null);

        assertNotNull(result);
        assertEquals(entry1, result);
    }

    @Test
    void testEntryWithDeleted() throws Exception {
        EntryInactiveReason reason1 =
                new EntryInactiveReasonBuilder()
                        .type(InactiveReasonType.DELETED)
                        .mergeDemergeTosAdd("P88888")
                        .build();
        UniProtKBEntry entry1 = new UniProtKBEntryBuilder("P99999", "ID_99999", reason1).build();

        InactiveEntryAggregationMapper mapper = new InactiveEntryAggregationMapper();
        UniProtKBEntry result = mapper.call(entry1, null);

        assertNotNull(result);
        assertEquals(entry1, result);
    }
}

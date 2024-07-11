package org.uniprot.store.indexer.uniprot.mockers;

import java.util.*;

import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;

/**
 * This class is responsible to create mock objects for inactive UniProt entries.
 *
 * @author lgonzales
 */
public class InactiveEntryMocker {

    public enum InactiveType {
        DELETED,
        MERGED,
        DEMERGED
    }

    private static final Map<InactiveType, List<InactiveUniProtEntry>> entryMap = new HashMap<>();

    public static final String ACTIVE_ACESSION = "P21802";
    public static final String DELETED = "deleted";
    public static final String MERGED = "merged";

    // Initialize Mocked InactiveUniProtEntry
    static {
        List<InactiveUniProtEntry> deletedEntries = new ArrayList<>();
        deletedEntries.add(
                InactiveUniProtEntry.from(
                        "I8FBX0",
                        "I8FBX0_MYCAB",
                        DELETED,
                        "UPI0001661588",
                        null,
                        "SOURCE_DELETION_EMBL"));
        deletedEntries.add(
                InactiveUniProtEntry.from(
                        "I8FBX1",
                        "I8FBX1_YERPE",
                        DELETED,
                        "UPI00000DCD3D",
                        null,
                        "PROTEOME_REDUNDANCY"));
        deletedEntries.add(
                InactiveUniProtEntry.from(
                        "I8FBX2", "I8FBX2_YERPE", DELETED, "UPI000012CEBB", null, null));
        entryMap.put(InactiveType.DELETED, deletedEntries);

        List<InactiveUniProtEntry> mergedEntries = new ArrayList<>();
        mergedEntries.add(
                InactiveUniProtEntry.from(
                        "Q14301", "Q14301_FGFR2", MERGED, null, ACTIVE_ACESSION, null));
        mergedEntries.add(
                InactiveUniProtEntry.from(
                        "B4DFC2", "B4DFC2_FGFR2", MERGED, null, ACTIVE_ACESSION, null));
        mergedEntries.add(
                InactiveUniProtEntry.from("F8VPU5", "F8VPU5_BRCA2", MERGED, null, "P97929", null));
        entryMap.put(InactiveType.MERGED, mergedEntries);

        List<InactiveUniProtEntry> demergedEntries = new ArrayList<>();
        demergedEntries.add(
                InactiveUniProtEntry.from(
                        "Q00007", "FGFR2_HUMAN", MERGED, null, ACTIVE_ACESSION, null));
        demergedEntries.add(
                InactiveUniProtEntry.from("Q00007", "FGFR2_HUMAN", MERGED, null, "P63151", null));
        entryMap.put(
                InactiveType.DEMERGED,
                Collections.singletonList(InactiveUniProtEntry.merge(demergedEntries)));
    }

    public static List<InactiveUniProtEntry> create(InactiveType inactiveType) {
        return entryMap.get(inactiveType);
    }
}

package org.uniprot.store.indexer.uniprot.inactiveentry;


import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

public class InactiveUniProtEntry {

    private final String accession;
    private final String id;
    private final String reason;
    private final List<String> mergedOrDemergedAccessions;
    private static final String DEMERGED = "demerged";


    public InactiveUniProtEntry(String accession, String id, String reason, List<String> mergedOrDemergedAccessions) {
        super();
        this.accession = accession;
        this.id = id;
        this.reason = reason;
        this.mergedOrDemergedAccessions = mergedOrDemergedAccessions;
    }

    public String getAccession() {
        return accession;
    }

    public String getId() {
        return id;
    }

    public String getReason() {
        return reason;
    }

    public List<String> getMergedOrDemergedAccessions() {
        return mergedOrDemergedAccessions;
    }

    public String getInactiveReason() {
        String inactiveReason = reason;
        if (!this.mergedOrDemergedAccessions.isEmpty()) {
            inactiveReason += ":" + String.join(",", this.mergedOrDemergedAccessions);
        }
        return inactiveReason;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(accession).append(",")
                .append(id).append(",")
                .append(reason).append(",")
                .append(mergedOrDemergedAccessions);

        return sb.toString();
    }

    public static InactiveUniProtEntry from(String accession, String id, String reason, String mergedToAccesion) {
        String name = id;
        if (accession.equals(name)) {
            name = "";
        } else if (name == null) {
            name = "";
        }
        List<String> mergedToAccesionList = new ArrayList<>();
        if (!Strings.isNullOrEmpty(mergedToAccesion) && !mergedToAccesion.equalsIgnoreCase("-")) {
            mergedToAccesionList.add(mergedToAccesion);
        }
        return new InactiveUniProtEntry(accession, name, reason, mergedToAccesionList);
    }

    public static InactiveUniProtEntry merge(List<InactiveUniProtEntry> demergedEntries) {
        if (demergedEntries.isEmpty()) {
            return null;
        } else if (demergedEntries.size() == 1)
            return demergedEntries.get(0);
        else {
            List<String> accessions = new ArrayList<>();
            demergedEntries.stream()
                    .map(InactiveUniProtEntry::getMergedOrDemergedAccessions)
                    .forEach(accessions::addAll);

            return new InactiveUniProtEntry(demergedEntries.get(0).getAccession(),
                                            demergedEntries.get(0).getId(), DEMERGED, accessions);
        }
    }
}

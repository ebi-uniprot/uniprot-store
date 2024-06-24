package org.uniprot.store.indexer.uniprot.inactiveentry;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Strings;

public class InactiveUniProtEntry {

    private final String accession;
    private final String id;
    private final String reason;
    private final String deletedReason;
    private final String uniParcDeleted;
    private final List<String> mergedOrDemergedAccessions;
    private static final String DEMERGED = "demerged";

    public InactiveUniProtEntry(
            String accession,
            String id,
            String reason,
            String uniParcDeleted,
            List<String> mergedOrDemergedAccessions,
            String deletedReason) {
        super();
        this.accession = accession;
        this.id = id;
        this.reason = reason;
        this.uniParcDeleted = uniParcDeleted;
        this.mergedOrDemergedAccessions = mergedOrDemergedAccessions;
        this.deletedReason = deletedReason;
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

    public String getDeletedReason() {
        return deletedReason;
    }

    public String getUniParcDeleted() {
        return uniParcDeleted;
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
        sb.append(accession)
                .append(",")
                .append(id)
                .append(",")
                .append(reason)
                .append(",")
                .append(uniParcDeleted)
                .append(",")
                .append(mergedOrDemergedAccessions);

        return sb.toString();
    }

    public static InactiveUniProtEntry from(
            String accession,
            String id,
            String reason,
            String uniParcDeleted,
            String mergedToAccesion,
            String deletedReason) {
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
        return new InactiveUniProtEntry(
                accession, name, reason, uniParcDeleted, mergedToAccesionList, deletedReason);
    }

    public static InactiveUniProtEntry merge(List<InactiveUniProtEntry> demergedEntries) {
        if (demergedEntries.isEmpty()) {
            return null;
        } else if (demergedEntries.size() == 1) return demergedEntries.get(0);
        else {
            List<String> accessions = new ArrayList<>();
            demergedEntries.stream()
                    .map(InactiveUniProtEntry::getMergedOrDemergedAccessions)
                    .forEach(accessions::addAll);

            return new InactiveUniProtEntry(
                    demergedEntries.get(0).getAccession(),
                    demergedEntries.get(0).getId(),
                    DEMERGED,
                    demergedEntries.get(0).uniParcDeleted,
                    accessions,
                    null);
        }
    }
}

package org.uniprot.store.indexer.uniparc.mockers;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.uniprot.store.indexer.uniparc.mockers.UniParcEntryMocker.*;

public class UniParcCrossReferenceMocker {
    private UniParcCrossReferenceMocker() {}

    // UniParcCrossReference and UniParcCrossReferencePair mockers

    public static UniParcCrossReference createUniParcCrossReference(UniParcDatabase database) {
        return new UniParcCrossReferenceBuilder()
                .versionI(3)
                .database(database)
                .id("id-" + database.name())
                .version(7)
                .active(true)
                .build();
    }

    public static UniParcCrossReference createUniParcCrossReference(
            UniParcDatabase database, String id, Integer taxId, boolean active) {
        return new UniParcCrossReferenceBuilder()
                .database(database)
                .id(id)
                .versionI(1)
                .version(7)
                .active(active)
                .created(LocalDate.of(2017, 2, 12))
                .lastUpdated(LocalDate.of(2017, 4, 23))
                .organism(getOrganism(taxId))
                .proteinName(PROTEIN_NAME)
                .geneName("Gel")
                .proteomeId("UPI")
                .component("com")
                .chain("chain")
                .build();
    }

    public static List<UniParcCrossReference> createCrossReferences(int count) {
        List<UniParcCrossReference> xrefs = new ArrayList<>();
        int crossRefSize = createUniParcCrossReferences(1).size();
        for (int i = 1;
                i <= count;
                i += crossRefSize) { // increase by createUniParcCrossReferences size
            xrefs.addAll(createUniParcCrossReferences(i));
        }
        return xrefs.subList(0, count);
    }

    public static List<UniParcCrossReference> createUniParcCrossReferences(int qualifier) {
        UniParcCrossReference xref1 =
                new UniParcCrossReferenceBuilder()
                        .versionI(3)
                        .database(UniParcDatabase.SWISSPROT)
                        .id(getName("P100", qualifier))
                        .version(7)
                        .active(true)
                        .ncbiGi(NCBI_GI + qualifier)
                        .created(LocalDate.of(2017, 5, 17))
                        .lastUpdated(LocalDate.of(2017, 2, 27))
                        .proteinName(getName(PROTEIN_NAME, qualifier))
                        .geneName(getName("geneName", qualifier))
                        .proteomeId(getName("UP1234567", qualifier))
                        .organism(getOrganism(7787L))
                        .component(getName("component", qualifier))
                        .chain("chain")
                        .build();

        UniParcCrossReference xref2 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.TREMBL)
                        .id(getName("P123", qualifier))
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(getName("anotherProteinName", qualifier))
                        .organism(getOrganism(9606L))
                        .proteomeId("UP000005640")
                        .component("chromosome")
                        .build();

        UniParcCrossReference xref3 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.REFSEQ)
                        .id(getName("WP_1688932", qualifier))
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .build();

        UniParcCrossReference xref4 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.EMBL)
                        .id("embl" + qualifier)
                        .version(7)
                        .active(true)
                        .ncbiGi(NCBI_GI + qualifier)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(PROTEIN_NAME + qualifier)
                        .build();

        UniParcCrossReference xref5 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.UNIMES)
                        .id("unimes" + qualifier)
                        .version(7)
                        .ncbiGi(NCBI_GI + qualifier)
                        .active(false)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(PROTEIN_NAME + qualifier)
                        .build();

        // common db xref
        UniParcCrossReference xref6 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.VECTORBASE)
                        .id("common-vector")
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName("common-vector-proteinName" + qualifier)
                        .build();
        return List.of(xref1, xref2, xref3, xref4, xref5, xref6);
    }

    public static List<UniParcCrossReferencePair> createUniParcCrossReferencePairs(
            String uniParcId, int xrefCount, int groupSize) {
        List<UniParcCrossReferencePair> uniParcCrossReferencePairs = new ArrayList<>();
        List<UniParcCrossReference> crossReferences = createCrossReferences(xrefCount);
        for (int i = 0, batchId = 0; i < crossReferences.size(); i += groupSize, batchId++) {
            int end = Math.min(i + groupSize, crossReferences.size());
            List<UniParcCrossReference> xrefBatch = crossReferences.subList(i, end);
            uniParcCrossReferencePairs.add(
                    new UniParcCrossReferencePair(uniParcId + "_" + batchId, xrefBatch));
        }
        return uniParcCrossReferencePairs;
    }
}

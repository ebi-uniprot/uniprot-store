package org.uniprot.store.indexer.uniparc.mockers;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Location;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.util.PairImpl;

/**
 * @author lgonzales
 * @since 18/06/2020
 */
public class UniParcEntryMocker {

    public static final String PROTEIN_NAME = "proteinName";
    public static final String NCBI_GI = "ncbiGi";

    private UniParcEntryMocker() {}

    public static UniParcEntry createEntry(int i, String upiRef) {
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, i).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        List<UniParcCrossReference> xrefs = getXrefs(i);

        List<SequenceFeature> seqFeatures = new ArrayList<>();
        Arrays.stream(SignatureDbType.values())
                .forEach(signatureType -> seqFeatures.add(getSeqFeature(i, signatureType)));
        return new UniParcEntryBuilder()
                .uniParcId(new UniParcIdBuilder(getName(upiRef, i)).build())
                .uniParcCrossReferencesSet(xrefs)
                .sequence(sequence)
                .sequenceFeaturesSet(seqFeatures)
                .build();
    }

    public static UniParcEntry createSimpleEntry(int i, String upiRef) {
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, i).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.ENSEMBL_VERTEBRATE)
                        .id(getName("WP_1688932", i))
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .build();

        return new UniParcEntryBuilder()
                .uniParcId(new UniParcIdBuilder(getName(upiRef, i)).build())
                .uniParcCrossReferencesAdd(xref)
                .sequence(sequence)
                .build();
    }

    static Organism getOrganism(long taxId) {
        return new OrganismBuilder().taxonId(taxId).scientificName("Name " + taxId).build();
    }

    public static SequenceFeature getSeqFeature(int i, SignatureDbType signatureDbType) {
        List<Location> locations = Arrays.asList(new Location(12, 23), new Location(45, 89));
        InterProGroup domain =
                new InterProGroupBuilder()
                        .name(getName("Inter Pro Name", i))
                        .id(getName("IP0000", i))
                        .build();
        return new SequenceFeatureBuilder()
                .interproGroup(domain)
                .signatureDbType(signatureDbType)
                .signatureDbId(getName("SIG0000", i))
                .locationsSet(locations)
                .build();
    }

    public static UniParcCrossReference getXref(UniParcDatabase database) {
        return new UniParcCrossReferenceBuilder()
                .versionI(3)
                .database(database)
                .id("id-" + database.name())
                .version(7)
                .active(true)
                .build();
    }

    public static List<UniParcCrossReference> getXrefs(int i) {
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .versionI(3)
                        .database(UniParcDatabase.SWISSPROT)
                        .id(getName("P100", i))
                        .version(7)
                        .active(true)
                        .ncbiGi(NCBI_GI + i)
                        .created(LocalDate.of(2017, 5, 17))
                        .lastUpdated(LocalDate.of(2017, 2, 27))
                        .proteinName(getName(PROTEIN_NAME, i))
                        .geneName(getName("geneName", i))
                        .proteomeId(getName("UP1234567", i))
                        .organism(getOrganism(7787L))
                        .component(getName("component", i))
                        .chain("chain")
                        .build();

        UniParcCrossReference xref2 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.TREMBL)
                        .id(getName("P123", i))
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(getName("anotherProteinName", i))
                        .organism(getOrganism(9606L))
                        .proteomeId("UP000005640")
                        .component("chromosome")
                        .build();

        UniParcCrossReference xref3 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.REFSEQ)
                        .id(getName("WP_1688932", i))
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .build();

        return Arrays.asList(xref, xref2, xref3);
    }

    public static UniParcEntry createEntry(
            String id, int sequenceLength, List<UniParcCrossReference> crossReferences) {
        UniParcEntryBuilder builder = new UniParcEntryBuilder();
        StringBuilder sequence = new StringBuilder();
        IntStream.range(0, sequenceLength).forEach(i -> sequence.append("A"));
        Sequence uniSeq = new SequenceBuilder(sequence.toString()).build();
        builder.uniParcId(id).sequence(uniSeq).uniParcCrossReferencesSet(crossReferences);
        return builder.build();
    }

    public static UniParcCrossReference getXref(
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

    static String getName(String prefix, int i) {
        if (i < 10) {
            return prefix + "0" + i;
        } else return prefix + i;
    }

    public static UniParcEntry appendMoreXRefs(UniParcEntry entry, int i) {
        List<UniParcCrossReference> xrefs = getMoreXrefs(i);
        UniParcEntryBuilder builder = UniParcEntryBuilder.from(entry);
        xrefs.forEach(xref -> builder.uniParcCrossReferencesAdd(xref));
        return builder.build();
    }

    private static List<UniParcCrossReference> getMoreXrefs(int i) {
        UniParcCrossReference xref1 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.EMBL)
                        .id("embl" + i)
                        .version(7)
                        .active(true)
                        .ncbiGi(NCBI_GI + i)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(PROTEIN_NAME + i)
                        .build();

        UniParcCrossReference xref2 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.UNIMES)
                        .id("unimes" + i)
                        .version(7)
                        .ncbiGi(NCBI_GI + i)
                        .active(false)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName(PROTEIN_NAME + i)
                        .build();

        // common db xref
        UniParcCrossReference xref3 =
                new UniParcCrossReferenceBuilder()
                        .versionI(1)
                        .database(UniParcDatabase.VECTORBASE)
                        .id("common-vector")
                        .version(7)
                        .active(true)
                        .created(LocalDate.of(2017, 2, 12))
                        .lastUpdated(LocalDate.of(2017, 4, 23))
                        .proteinName("common-vector-proteinName" + i)
                        .build();
        return List.of(xref1, xref2, xref3);
    }

    public static UniParcEntryLight createUniParcEntryLight(int i, String prefix) {
        String uniParcId = getName(prefix, i);
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, i).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        List<UniParcCrossReference> xrefObjects = new ArrayList<>(getXrefs(i));
        xrefObjects.addAll(getMoreXrefs(i));
        List<String> xrefs =
                xrefObjects.stream().map(xref -> getUniParcXRefId(uniParcId, xref)).toList();

        List<SequenceFeature> seqFeatures = new ArrayList<>();
        Arrays.stream(SignatureDbType.values())
                .forEach(signatureType -> seqFeatures.add(getSeqFeature(i, signatureType)));
        List<CommonOrganism> commonTaxons = getCommonTaxons();
        return new UniParcEntryLightBuilder()
                .uniParcId(uniParcId)
                .commonTaxonsSet(commonTaxons)
                .uniParcCrossReferencesSet(xrefs)
                .sequence(sequence)
                .sequenceFeaturesSet(seqFeatures)
                .oldestCrossRefCreated(LocalDate.now())
                .mostRecentCrossRefUpdated(LocalDate.now())
                .build();
    }

    private static List<CommonOrganism> getCommonTaxons() {
        return List.of(
                new CommonOrganismBuilder()
                        .topLevel("cellular organisms")
                        .commonTaxon("Bacteria")
                        .build(),
                new CommonOrganismBuilder()
                        .topLevel("other entries")
                        .commonTaxon("plasmids")
                        .build());
    }

    public static String getUniParcXRefId(String uniParcId, UniParcCrossReference crossRef) {
        String id = crossRef.getId();
        String databaseType = crossRef.getDatabase().name();
        return uniParcId + "-" + databaseType + "-" + id;
    }

    public static List<PairImpl<String, UniParcCrossReference>> getXrefPairs(
            String uniParcId, int i) {
        List<UniParcCrossReference> crossRefs = new ArrayList<>(getXrefs(i));
        crossRefs.addAll(getMoreXrefs(i));
        List<PairImpl<String, UniParcCrossReference>> idXrefPairs =
                crossRefs.stream()
                        .map(xref -> new PairImpl<>(getUniParcXRefId(uniParcId, xref), xref))
                        .toList();
        return idXrefPairs;
    }

    public static UniParcEntryLight convertToUniParcEntryLight(UniParcEntry entry) {
        String uniParcId = entry.getUniParcId().getValue();
        List<String> xrefIds =
                entry.getUniParcCrossReferences().stream()
                        .map(xref -> getUniParcXRefId(uniParcId, xref))
                        .toList();
        LinkedHashSet<String> uniProtKBAccession =
                entry.getUniParcCrossReferences().stream()
                        .filter(UniParcEntryMocker::isUniProtDatabase)
                        .map(CrossReference::getId)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        LinkedHashSet<Organism> organisms =
                entry.getUniParcCrossReferences().stream()
                        .map(UniParcCrossReference::getOrganism)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        return new UniParcEntryLightBuilder()
                .uniParcId(uniParcId)
                .commonTaxonsSet(getCommonTaxons())
                .uniProtKBAccessionsSet(uniProtKBAccession)
                .uniParcCrossReferencesSet(xrefIds)
                .organismsSet(organisms)
                .sequence(entry.getSequence())
                .sequenceFeaturesSet(entry.getSequenceFeatures())
                .oldestCrossRefCreated(entry.getOldestCrossRefCreated())
                .mostRecentCrossRefUpdated(entry.getMostRecentCrossRefUpdated())
                .build();
    }

    private static boolean isUniProtDatabase(UniParcCrossReference xref) {
        return UniParcDatabase.SWISSPROT.equals(xref.getDatabase())
                || UniParcDatabase.TREMBL.equals(xref.getDatabase())
                || UniParcDatabase.SWISSPROT_VARSPLIC.equals(xref.getDatabase());
    }
}

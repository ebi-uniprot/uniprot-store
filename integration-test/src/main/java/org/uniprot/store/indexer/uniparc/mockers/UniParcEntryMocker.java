package org.uniprot.store.indexer.uniparc.mockers;

import static org.uniprot.store.indexer.uniparc.mockers.UniParcCrossReferenceMocker.createCrossReferences;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.*;
import org.uniprot.core.uniparc.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;

/**
 * @author lgonzales
 * @since 18/06/2020
 */
public class UniParcEntryMocker {

    public static final String PROTEIN_NAME = "proteinName";
    public static final String NCBI_GI = "ncbiGi";

    private UniParcEntryMocker() {}

    public static UniParcEntry createUniParcEntry(int qualifier, String uniParcPrefix) {
        return createUniParcEntry(qualifier, uniParcPrefix, 3);
    }

    public static UniParcEntry createUniParcEntry(
            int qualifier, String uniParcPrefix, int xrefCount) {
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, qualifier).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        List<UniParcCrossReference> xrefs = createCrossReferences(qualifier, xrefCount);

        List<SequenceFeature> seqFeatures = new ArrayList<>();
        Arrays.stream(SignatureDbType.values())
                .forEach(signatureType -> seqFeatures.add(getSeqFeature(qualifier, signatureType)));
        return new UniParcEntryBuilder()
                .uniParcId(new UniParcIdBuilder(getName(uniParcPrefix, qualifier)).build())
                .uniParcCrossReferencesSet(xrefs)
                .sequence(sequence)
                .sequenceFeaturesSet(seqFeatures)
                .build();
    }

    public static UniParcEntry createUniParcEntry(
            String id, int sequenceLength, List<UniParcCrossReference> crossReferences) {
        UniParcEntryBuilder builder = new UniParcEntryBuilder();
        StringBuilder sequence = new StringBuilder();
        IntStream.range(0, sequenceLength).forEach(i -> sequence.append("A"));
        Sequence uniSeq = new SequenceBuilder(sequence.toString()).build();
        builder.uniParcId(id).sequence(uniSeq).uniParcCrossReferencesSet(crossReferences);
        return builder.build();
    }

    public static UniParcEntryLight createUniParcEntryLight(
            int qualifier, String prefix, int xrefCount) {
        String uniParcId = getName(prefix, qualifier);
        StringBuilder seq = new StringBuilder("MLMPKRTKYR");
        IntStream.range(0, qualifier).forEach(j -> seq.append("A"));
        Sequence sequence = new SequenceBuilder(seq.toString()).build();
        List<SequenceFeature> seqFeatures = new ArrayList<>();
        Arrays.stream(SignatureDbType.values())
                .forEach(signatureType -> seqFeatures.add(getSeqFeature(qualifier, signatureType)));
        List<CommonOrganism> commonTaxons = getCommonTaxons();
        return new UniParcEntryLightBuilder()
                .uniParcId(uniParcId)
                .commonTaxonsSet(commonTaxons)
                .crossReferenceCount(xrefCount)
                .uniProtKBAccessionsAdd(getName("P123", qualifier))
                .sequence(sequence)
                .sequenceFeaturesSet(seqFeatures)
                .oldestCrossRefCreated(LocalDate.now())
                .mostRecentCrossRefUpdated(LocalDate.now())
                .build();
    }

    public static UniParcEntryLight convertToUniParcEntryLight(UniParcEntry entry) {
        String uniParcId = entry.getUniParcId().getValue();
        int numberOfXrefs = entry.getUniParcCrossReferences().size();
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
                .crossReferenceCount(numberOfXrefs)
                .organismsSet(organisms)
                .sequence(entry.getSequence())
                .sequenceFeaturesSet(entry.getSequenceFeatures())
                .oldestCrossRefCreated(entry.getOldestCrossRefCreated())
                .mostRecentCrossRefUpdated(entry.getMostRecentCrossRefUpdated())
                .build();
    }

    public static UniParcEntryLight createEntryLightWithSequenceLength(
            String uniParcId, int sequenceLength, int xrefCount) {
        UniParcEntryLightBuilder builder = new UniParcEntryLightBuilder();
        StringBuilder sequence = new StringBuilder();
        IntStream.range(0, sequenceLength).forEach(i -> sequence.append("A"));
        Sequence uniSeq = new SequenceBuilder(sequence.toString()).build();
        builder.uniParcId(uniParcId).sequence(uniSeq).crossReferenceCount(xrefCount);
        return builder.build();
    }

    public static String getName(String prefix, int i) {
        return String.format(prefix + "%02d", i);
    }

    public static Organism getOrganism(long taxId) {
        return new OrganismBuilder().taxonId(taxId).scientificName("Name " + taxId).build();
    }

    public static SequenceFeature getSeqFeature(int i, SignatureDbType signatureDbType) {
        List<SequenceFeatureLocation> locations =
                Arrays.asList(
                        new SequenceFeatureLocationBuilder().range(12, 23).alignment("55M").build(),
                        new SequenceFeatureLocationBuilder().range(45, 89).build());
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

    private static boolean isUniProtDatabase(UniParcCrossReference xref) {
        return UniParcDatabase.SWISSPROT.equals(xref.getDatabase())
                || UniParcDatabase.TREMBL.equals(xref.getDatabase())
                || UniParcDatabase.SWISSPROT_VARSPLIC.equals(xref.getDatabase());
    }
}

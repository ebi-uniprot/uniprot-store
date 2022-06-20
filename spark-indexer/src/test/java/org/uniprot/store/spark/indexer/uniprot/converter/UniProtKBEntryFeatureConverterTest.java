package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.feature.FeatureLocation;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.feature.*;
import org.uniprot.core.uniprotkb.feature.impl.AlternativeSequenceBuilder;
import org.uniprot.core.uniprotkb.feature.impl.LigandBuilder;
import org.uniprot.core.uniprotkb.feature.impl.LigandPartBuilder;
import org.uniprot.core.uniprotkb.feature.impl.UniProtKBFeatureBuilder;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtKBEntryFeatureConverterTest {

    @Test
    void convertFeatureWithFilteredProteinsWith() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        FeatureLocation location = new FeatureLocation(2, 8);
        UniProtKBFeature feature =
                new UniProtKBFeatureBuilder()
                        .type(UniprotKBFeatureType.NON_TER)
                        .location(location)
                        .build();

        List<UniProtKBFeature> features = Collections.singletonList(feature);

        converter.convertFeature(features, document);

        assertEquals(Collections.emptyList(), document.proteinsWith);
    }

    @Test
    void convertFeature() {
        UniProtDocument document = new UniProtDocument();
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();

        List<UniProtKBFeature> features = Collections.singletonList(getFeature());

        converter.convertFeature(features, document);

        assertTrue(document.featuresMap.containsKey("ft_chain"));
        List<String> chainValue =
                Arrays.asList(
                        "CHAIN",
                        "FT12345",
                        "dbSNP-DBSNP-12345",
                        "description value",
                        "DBSNP-12345");
        assertEquals(new HashSet<>(chainValue), document.featuresMap.get("ft_chain"));

        assertTrue(document.featureEvidenceMap.containsKey("ftev_chain"));
        List<String> chainEvidenceValue = Arrays.asList("manual", "ECO_0000255");
        assertEquals(
                new HashSet<>(chainEvidenceValue), document.featureEvidenceMap.get("ftev_chain"));

        assertTrue(document.featureLengthMap.containsKey("ftlen_chain"));
        List<Integer> chainLengthValue = Collections.singletonList(7);
        assertEquals(new HashSet<>(chainLengthValue), document.featureLengthMap.get("ftlen_chain"));

        assertEquals(5, document.content.size());
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "CHAIN",
                                "FT12345",
                                "dbSNP-DBSNP-12345",
                                "description value",
                                "DBSNP-12345")),
                document.content);

        assertEquals(Collections.singletonList(14), document.proteinsWith);
    }


    @Test
    void convertBindingFeature() {
        UniProtDocument document = new UniProtDocument();
    

        List<UniProtKBFeature> features = Collections.singletonList(createBindingFeature());
        UniProtEntryFeatureConverter converter = new UniProtEntryFeatureConverter();
        converter.convertFeature(features, document);

        assertTrue(document.featuresMap.containsKey("ft_binding"));
        List<String> chainValue =
                List.of(
                		"A1", "CHEBI:29180", "CHEBI:83071", "ChEBI-CHEBI:29180",
                		"nucleotidyl-adenosine residue", "ChEBI-CHEBI:83071", "BINDING", "tRNA(Thr)"
);
         assertEquals(new HashSet<>(chainValue), document.featuresMap.get("ft_binding"));

        assertTrue(document.featureEvidenceMap.containsKey("ftev_binding"));
        List<String> chainEvidenceValue = Arrays.asList("manual", "ECO_0000255");
        assertEquals(
                new HashSet<>(chainEvidenceValue), document.featureEvidenceMap.get("ftev_binding"));

        assertTrue(document.featureLengthMap.containsKey("ftlen_binding"));
        List<Integer> bindingLengthValue = Collections.singletonList(12);
        assertEquals(new HashSet<>(bindingLengthValue), document.featureLengthMap.get("ftlen_binding"));

        assertEquals(8, document.content.size());
        assertEquals(
                new HashSet<>(
                		   List.of(
                           		"A1", "CHEBI:29180", "CHEBI:83071","ChEBI-CHEBI:29180",
                           		"nucleotidyl-adenosine residue", "ChEBI-CHEBI:83071", "BINDING", "tRNA(Thr)")),
                document.content);

        assertEquals(
                Collections.singletonList(ProteinsWith.BINDING_SITE.getValue()), document.proteinsWith);
    }

    
    private static UniProtKBFeature getFeature() {
        AlternativeSequence alternativeSequence =
                new AlternativeSequenceBuilder()
                        .original("original value")
                        .alternativeSequencesAdd("alternative value")
                        .build();

        CrossReference<UniprotKBFeatureDatabase> xrefs =
                new CrossReferenceBuilder<UniprotKBFeatureDatabase>()
                        .database(UniprotKBFeatureDatabase.DBSNP)
                        .id("DBSNP-12345")
                        .build();

        FeatureLocation location = new FeatureLocation(2, 8);
        List<Evidence> evidences = Collections.singletonList(createEvidence());
        return new UniProtKBFeatureBuilder()
                .type(UniprotKBFeatureType.CHAIN)
                .alternativeSequence(alternativeSequence)
                .featureCrossReferenceAdd(xrefs)
                .description("description value")
                .evidencesSet(evidences)
                .featureId("FT12345")
                .location(location)
                .build();
    }

    private UniProtKBFeature createBindingFeature() {
    	LigandPart ligandPart =createLigandPart("nucleotidyl-adenosine residue","ChEBI:CHEBI:83071", null, null);
        
        
        Ligand ligand =createLigand("tRNA(Thr)", "ChEBI:CHEBI:29180", "A1", null);
        CrossReference<UniprotKBFeatureDatabase> xref1 =
                new CrossReferenceBuilder<UniprotKBFeatureDatabase>()
                        .database(UniprotKBFeatureDatabase.CHEBI)
                        .id("CHEBI:29180")
                        .build();
        CrossReference<UniprotKBFeatureDatabase> xref2 =
                new CrossReferenceBuilder<UniprotKBFeatureDatabase>()
                        .database(UniprotKBFeatureDatabase.CHEBI)
                        .id("CHEBI:83071")
                        .build();
    	
    	return new UniProtKBFeatureBuilder()
        .type(UniprotKBFeatureType.BINDING)
        .location(new FeatureLocation(23, 34))
        .evidencesAdd(createEvidence())
        .ligand(ligand)
        .ligandPart(ligandPart)
        .featureCrossReferenceAdd(xref1)
        .featureCrossReferenceAdd(xref2)
        .build();
    }
    private Ligand createLigand(String name, String id, String label, String note) {
		return new LigandBuilder()
        		.name(name)
        		.id(id)
        		.label(label)
        		.note(note)
        		.build();
    }
    
    private LigandPart createLigandPart(String name, String id, String label, String note) {
        return new LigandPartBuilder()
        		.name(name)
        		.id(id)
        		.label(label)
        		.note(note)
        		.build();
      
    }
 
    
    private static Evidence createEvidence() {
        return new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000255)
                .databaseName("PROSITE-ProRule")
                .databaseId("PRU10020")
                .build();
    }
}

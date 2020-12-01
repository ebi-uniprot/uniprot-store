package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.description.*;
import org.uniprot.core.uniprotkb.description.impl.*;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtKBEntryProteinDescriptionConverterTest {

    @Test
    void convertCompleteProteinDescription() {
        UniProtDocument document = new UniProtDocument();

        ProteinDescription proteinDescription = getProteinDescription();

        UniProtEntryProteinDescriptionConverter converter =
                new UniProtEntryProteinDescriptionConverter();
        converter.convertProteinDescription(proteinDescription, document);

        List<String> indexedNames =
                Arrays.asList(
                        "rec full Name",
                        "recommended short name",
                        "sub full Name",
                        "a full alt Name",
                        "short alt name1",
                        "containsrec full Name",
                        "containsrecommended short name",
                        "containsa full alt Name",
                        "containsshort alt name1",
                        "contains cd antigen",
                        "contains allergen",
                        "contains inn antigen",
                        "contains biotech",
                        "includesrec full Name",
                        "includesrecommended short name",
                        "includesa full alt Name",
                        "includesshort alt name1",
                        "includes cd antigen",
                        "includes allergen",
                        "includes inn antigen",
                        "includes biotech",
                        "main allergen",
                        "main biotech",
                        "main cd antigen",
                        "main inn antigen");

        assertEquals(25, document.proteinNames.size());
        assertEquals(indexedNames, document.proteinNames);

        assertEquals(30, document.proteinsNamesSort.length());
        assertEquals("rec full Name recommended shor", document.proteinsNamesSort);

        assertEquals(
                Arrays.asList(
                        "1.2.3.4", "1.2.3.5", "1.2.3.3", "1.2.3.4", "1.2.3.3", "1.2.3.4",
                        "1.2.3.3"),
                document.ecNumbers);
        assertEquals(document.ecNumbersExact, document.ecNumbers);

        assertTrue(document.fragment);
        assertFalse(document.precursor);
    }

    @Test
    void convertPrecursorProteinDescription() {
        UniProtDocument document = new UniProtDocument();

        ProteinDescription description =
                new ProteinDescriptionBuilder()
                        .recommendedName(getRecommendedName(""))
                        .flag(FlagType.PRECURSOR)
                        .build();

        UniProtEntryProteinDescriptionConverter converter =
                new UniProtEntryProteinDescriptionConverter();
        converter.convertProteinDescription(description, document);

        assertFalse(document.fragment);
        assertTrue(document.precursor);
    }

    @Test
    void convertPrecursorAndFragmentProteinDescription() {
        UniProtDocument document = new UniProtDocument();

        ProteinDescription description =
                new ProteinDescriptionBuilder()
                        .recommendedName(getRecommendedName(""))
                        .flag(FlagType.FRAGMENTS_PRECURSOR)
                        .build();

        UniProtEntryProteinDescriptionConverter converter =
                new UniProtEntryProteinDescriptionConverter();
        converter.convertProteinDescription(description, document);

        assertTrue(document.fragment);
        assertTrue(document.precursor);
    }

    private static ProteinDescription getProteinDescription() {
        ProteinSection include =
                new ProteinSectionBuilder()
                        .recommendedName(getRecommendedName("includes"))
                        .alternativeNamesSet(createAltName("includes"))
                        .innNamesAdd(createName("includes inn antigen", "PRU100212"))
                        .allergenName(createName("includes allergen", "PRU10023"))
                        .biotechName(createName("includes biotech", "PRU10024"))
                        .cdAntigenNamesAdd(createName("includes cd antigen", "PRU10025"))
                        .build();

        ProteinSection contain =
                new ProteinSectionBuilder()
                        .recommendedName(getRecommendedName("contains"))
                        .alternativeNamesSet(createAltName("contains"))
                        .innNamesAdd(createName("contains inn antigen", "PRU100212"))
                        .allergenName(createName("contains allergen", "PRU10023"))
                        .biotechName(createName("contains biotech", "PRU10024"))
                        .cdAntigenNamesAdd(createName("contains cd antigen", "PRU10025"))
                        .build();

        ProteinName recommendedName = getRecommendedName("");
        List<ProteinName> proteinAltNames = createAltName("");
        List<ProteinSubName> subNames = getSubmissionName();

        return new ProteinDescriptionBuilder()
                .allergenName(createName("main allergen", "PRU10023"))
                .alternativeNamesSet(proteinAltNames)
                .biotechName(createName("main biotech", "PRU10024"))
                .cdAntigenNamesAdd(createName("main cd antigen", "PRU10025"))
                .flag(FlagType.FRAGMENT)
                .includesAdd(include)
                .containsAdd(contain)
                .innNamesAdd(createName("main inn antigen", "PRU100212"))
                .recommendedName(recommendedName)
                .submissionNamesSet(subNames)
                .build();
    }

    private static ProteinName getRecommendedName(String from) {
        Name fullName = createName(from + "rec full Name", "PRU10026");
        List<Name> shortNames = createNameList(from + "recommended short name", "PRU10020");
        List<EC> ecNumbers = createECNumbers("1.2.3.4", 10);

        return new ProteinNameBuilder()
                .fullName(fullName)
                .shortNamesSet(shortNames)
                .ecNumbersSet(ecNumbers)
                .build();
    }

    private static List<ProteinSubName> getSubmissionName() {
        Name fullName1 = createName("sub full Name", "PRU10027");
        List<EC> ecNumbers1 = createECNumbers("1.2.3.5", 11);

        ProteinSubName subName =
                new ProteinSubNameBuilder().fullName(fullName1).ecNumbersSet(ecNumbers1).build();
        return Collections.singletonList(subName);
    }

    private static List<ProteinName> createAltName(String from) {
        Name fullName = createName(from + "a full alt Name", "PRU10022");
        List<Name> shortNames = createNameList(from + "short alt name1", "PRU10028");
        List<EC> ecNumbers = createECNumbers("1.2.3.3", 9);

        ProteinName alternativeName =
                new ProteinNameBuilder()
                        .fullName(fullName)
                        .shortNamesSet(shortNames)
                        .ecNumbersSet(ecNumbers)
                        .build();
        return Collections.singletonList(alternativeName);
    }

    private static List<Name> createNameList(String value, String id) {
        return Collections.singletonList(createName(value, id));
    }

    private static Name createName(String value, String id) {
        return new NameBuilder().value(value).evidencesAdd(createEvidence(2)).build();
    }

    private static List<EC> createECNumbers(String ec, int index) {
        return Collections.singletonList(
                new ECBuilder().value(ec).evidencesAdd(createEvidence(index)).build());
    }

    private static Evidence createEvidence(int index) {
        return new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000255)
                .databaseName("PROSITE-ProRule")
                .databaseId("PRU1002" + index)
                .build();
    }
}

package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryCommentsConverter.EXPERIMENTAL;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
class UniProtKBEntryConverterIT {
    private static final String CC_CATALYTIC_ACTIVITY = "cc_catalytic_activity";
    private static final String CC_ALTERNATIVE_PRODUCTS_FIELD = "cc_alternative_products";
    private static final String CC_SIMILARITY_FIELD = "cc_similarity";
    private static final String CCEV_SIMILARITY_FIELD = "ccev_similarity";
    private static final String FT_CONFLICT_FIELD = "ft_conflict";
    private static final String FTEV_CONFLICT_FIELD = "ftev_conflict";
    private static final String FTLEN_CHAIN_FIELD = "ftlen_chain";
    private static final Date d1Aug2000 =
            Date.from(LocalDate.of(2000, Month.AUGUST, 1).atStartOfDay(ZoneOffset.UTC).toInstant());
    private DateFormat dateFormat;
    private UniProtEntryConverter converter;

    @BeforeEach
    void setUp() {
        converter = new UniProtEntryConverter(new HashMap<>());
        dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
    }

    @Test
    void testConvertFullA0PHU1Entry() throws Exception {
        String file = "A0PHU1.trembl";
        UniProtKBEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals("A0PHU1", doc.accession);
        assertEquals(1, doc.id.size());
        assertTrue(doc.id.contains("A0PHU1_9CICH"));
        assertFalse(doc.reviewed);
        assertEquals(1, doc.proteinNames.size());
        assertEquals("Cytochrome b", doc.proteinNames.get(0));
        assertEquals("Cytochrome b", doc.proteinsNamesSort);
        assertEquals(0, doc.ecNumbers.size());

        assertEquals("09-JAN-2007", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("07-JAN-2015", dateFormat.format(doc.lastModified).toUpperCase());

        assertEquals(4, doc.keywords.size());
        assertEquals("KW-9990", doc.keywords.get(0));
        assertEquals("Iron", doc.keywords.get(1));

        assertEquals(172543, doc.organismTaxId);

        assertEquals(1, doc.encodedIn.size());
        assertEquals("mitochondrion", doc.encodedIn.get(0));

        assertEquals(1, doc.organismHostIds.size());
        assertEquals(9539, doc.organismHostIds.get(0).intValue());

        assertEquals(92, doc.crossRefs.size());
        assertTrue(doc.crossRefs.contains("embl-AAY21541.1"));
        assertTrue(doc.crossRefs.contains("embl-AAY21540.1"));
        assertTrue(doc.crossRefs.contains("embl-AAY21540"));
        assertTrue(doc.crossRefs.contains("embl-AAY21541"));
        assertTrue(doc.crossRefs.contains("embl-Genomic_DNA"));
        assertTrue(doc.crossRefs.contains("embl-JOINED"));
        assertTrue(doc.crossRefs.contains("AAY21541.1"));
        assertTrue(doc.crossRefs.contains("AAY21541"));
        assertTrue(doc.crossRefs.contains("JOINED"));
        assertTrue(doc.crossRefs.contains("AAY21540.1"));
        assertTrue(doc.crossRefs.contains("AAY21540"));

        // mim
        assertTrue(doc.crossRefs.contains("mim-phenotype"));
        assertTrue(doc.crossRefs.contains("mim-616754"));
        assertTrue(doc.crossRefs.contains("phenotype"));
        assertTrue(doc.crossRefs.contains("616754"));

        // tcdb
        assertTrue(doc.crossRefs.contains("tcdb-8"));
        assertTrue(doc.crossRefs.contains("tcdb-8.A.94.1.2"));
        assertTrue(doc.crossRefs.contains("8.A.94.1.2"));
        assertTrue(doc.crossRefs.contains("8"));
        assertTrue(doc.content.contains("the adiponectin (adiponectin) family"));

        // hgnc
        assertTrue(doc.crossRefs.contains("hgnc-5984"));
        assertTrue(doc.crossRefs.contains("hgnc-HGNC:5984"));
        assertTrue(doc.crossRefs.contains("5984"));
        assertTrue(doc.crossRefs.contains("HGNC:5984"));

        // cdd
        assertTrue(doc.crossRefs.contains("cdd-1"));
        assertTrue(doc.crossRefs.contains("cdd-TM_EGFR-like"));
        assertTrue(doc.crossRefs.contains("cdd-cd12087"));
        assertTrue(doc.crossRefs.contains("1"));
        assertTrue(doc.crossRefs.contains("TM_EGFR-like"));
        assertTrue(doc.crossRefs.contains("cd12087"));

        // pirsf
        assertTrue(doc.crossRefs.contains("pirsf-PIRSF001138"));
        assertTrue(doc.crossRefs.contains("pirsf-1"));
        assertTrue(doc.crossRefs.contains("pirsf-Enteropeptidase"));
        assertTrue(doc.crossRefs.contains("1"));
        assertTrue(doc.crossRefs.contains("Enteropeptidase"));
        assertTrue(doc.crossRefs.contains("PIRSF001138"));

        assertEquals(13, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("interpro"));

        assertEquals(1, doc.referenceTitles.size());
        assertTrue(
                doc.referenceTitles
                        .get(0)
                        .startsWith("Phylogeny and biogeography of 91 species of heroine"));

        assertEquals(12, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.get(0).startsWith("Concheiro Perez G.A."));
        assertTrue(doc.referenceAuthors.get(1).startsWith("Bermingham E."));

        assertEquals(1, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("17045493"));

        assertEquals(2, doc.referenceCitationIds.size());
        assertTrue(doc.referenceCitationIds.contains("17045493"));

        assertEquals(2, doc.referenceDates.size());
        assertEquals("01-DEC-2004", dateFormat.format(doc.referenceDates.get(0)).toUpperCase());

        assertEquals(1, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Mol. Phylogenet. Evol."));

        assertEquals(3, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(
                doc.commentMap
                        .get(CC_SIMILARITY_FIELD)
                        .contains("SIMILARITY: Belongs to the cytochrome b family."));

        assertEquals(3, doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(3, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(
                doc.commentMap
                        .get(CC_SIMILARITY_FIELD)
                        .contains("SIMILARITY: Belongs to the cytochrome b family."));

        assertEquals(2, doc.cofactorChebi.size());
        assertTrue(doc.cofactorChebi.contains("heme"));

        assertEquals(1, doc.cofactorNote.size());
        assertTrue(doc.cofactorNote.contains("Binds 2 heme groups non-covalently."));

        assertEquals(1, doc.familyInfo.size());
        assertTrue(doc.familyInfo.contains("cytochrome b family"));

        assertEquals(42276, doc.seqMass);
        assertEquals(378, doc.seqLength);

        assertEquals(1, doc.scopes.size());
        assertTrue(doc.scopes.contains("NUCLEOTIDE SEQUENCE"));

        assertEquals(14, doc.goes.size());
        assertTrue(doc.goes.contains("respiratory chain"));

        assertEquals(2, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(2, doc.score);

        assertFalse(doc.isIsoform);
        assertNotNull(doc.suggests);
        assertEquals(1, doc.suggests.size());
        assertTrue(doc.suggests.containsAll(doc.proteinNames));
    }

    @Test
    void testConvertFullQ9EPI6Entry() throws Exception {
        String file = "Q9EPI6.sp";
        UniProtKBEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6", doc.accession);
        assertEquals(5, doc.secacc.size());
        assertEquals("Q7TSC6", doc.secacc.get(1));
        assertNull(doc.canonicalAccession);
        assertEquals(1, doc.id.size());
        assertTrue(doc.id.contains("NSMF_RAT"));
        assertTrue(doc.reviewed);

        assertEquals(5, doc.proteinNames.size());
        assertTrue(
                doc.proteinNames.contains(
                        "NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(
                doc.proteinNames.contains(
                        "Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(
                doc.proteinNames.contains(
                        "Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("NMDA receptor synaptonuclear s", doc.proteinsNamesSort);

        assertEquals(2, doc.ecNumbers.size());
        assertEquals(2, doc.ecNumbersExact.size());
        assertTrue(doc.ecNumbers.containsAll(List.of("2.7.10.2", "1.13.12.7")));
        assertTrue(doc.ecNumbersExact.containsAll(List.of("2.7.10.2", "1.13.12.7")));

        assertEquals("29-OCT-2014", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(4, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));
        List<String> keywordIds =
                doc.keywords.stream()
                        .filter(val -> val.startsWith("KW-"))
                        .collect(Collectors.toList());

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(10116, doc.organismTaxId);

        assertEquals(0, doc.encodedIn.size());
        assertEquals(0, doc.organismHostNames.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(216, doc.crossRefs.size());
        assertTrue(doc.crossRefs.contains("refseq-NM_001270626.1"));
        assertTrue(doc.crossRefs.contains("refseq-NM_001270626"));
        assertTrue(doc.crossRefs.contains("NM_001270626.1"));
        assertTrue(doc.crossRefs.contains("NM_001270626"));

        assertEquals(22, doc.databases.size());
        assertTrue(doc.databases.contains("refseq"));
        assertTrue(doc.databases.contains("ensembl"));

        assertEquals(7, doc.referenceTitles.size());
        assertTrue(
                doc.referenceTitles.contains(
                        "Characterization of the novel brain-specific protein Jacob."));

        assertEquals(56, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R."));
        assertTrue(doc.referenceAuthors.contains("Wray S."));
        assertTrue(doc.referenceAuthors.contains("The MGC Project Team"));

        assertEquals(7, doc.referenceCitationIds.size());
        assertTrue(doc.referenceCitationIds.contains("CI-73HJSSOHL8LGA"));
        assertTrue(doc.referenceCitationIds.contains("15489334"));

        assertEquals(5, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(7, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(d1Aug2000));

        assertEquals(5, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(18, doc.proteinsWith.size());
        assertTrue(doc.proteinsWith.contains(ProteinsWith.CHAIN.getValue()));
        assertTrue(doc.evidenceExperimental);
        assertEquals(17, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey("cc_function" + EXPERIMENTAL));
        assertTrue(doc.commentMap.containsKey(CC_CATALYTIC_ACTIVITY + EXPERIMENTAL));
        assertTrue(doc.commentMap.containsKey("cc_subunit" + EXPERIMENTAL));
        assertTrue(doc.commentMap.containsKey("cc_tissue_specificity" + EXPERIMENTAL));
        assertTrue(doc.commentMap.containsKey("cc_ptm" + EXPERIMENTAL));
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(
                doc.commentMap
                        .get(CC_SIMILARITY_FIELD)
                        .contains("SIMILARITY: Belongs to the NSMF family."));

        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(
                doc.commentMap
                        .get(CC_SIMILARITY_FIELD)
                        .contains("SIMILARITY: Belongs to the NSMF family."));

        assertEquals(14, doc.featuresMap.size());
        assertTrue(doc.featuresMap.containsKey(FT_CONFLICT_FIELD));
        assertTrue(doc.featuresMap.get(FT_CONFLICT_FIELD).contains("in Ref. 3; AAH87719"));

        assertEquals(1, doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertTrue(doc.commentMap.containsKey(CC_CATALYTIC_ACTIVITY));
        assertTrue(doc.commentMap.get(CC_CATALYTIC_ACTIVITY).contains("CHEBI:23367"));
        assertTrue(doc.commentMap.get(CC_CATALYTIC_ACTIVITY).contains("RHEA:10732"));

        assertEquals(26, doc.subcellLocationTerm.size());
        assertTrue(doc.subcellLocationTerm.contains("Nucleus envelope"));
        assertEquals(1, doc.subcellLocationNote.size());
        List<String> subcellTerm =
                doc.subcellLocationTerm.stream()
                        .filter(val -> !val.startsWith("SL-"))
                        .collect(Collectors.toList());

        assertEquals(5, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertTrue(doc.ap.contains("described"));
        assertEquals(4, doc.apAs.size());
        assertTrue(doc.apAs.contains("Additional isoforms seem to exist."));
        assertTrue(doc.apAs.contains("displayed"));

        assertEquals(5, doc.interactors.size());
        assertTrue(doc.interactors.contains("P27361"));

        assertEquals(1, doc.familyInfo.size());
        assertTrue(doc.familyInfo.contains("NSMF family"));

        assertEquals(60282, doc.seqMass);
        assertEquals(532, doc.seqLength);

        assertEquals(2, doc.rcTissue.size());
        assertTrue(doc.rcTissue.contains("Hippocampus"));

        assertEquals(2, doc.rcStrain.size());
        assertTrue(doc.rcStrain.contains("Wistar"));

        assertEquals(11, doc.scopes.size());
        assertTrue(doc.scopes.contains("SUBCELLULAR LOCATION"));

        assertEquals(50, doc.goes.size());
        assertTrue(doc.goes.contains("0030863"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);

        assertFalse(doc.isIsoform);
        assertNotNull(doc.suggests);
        assertEquals(9, doc.suggests.size());
        assertTrue(doc.suggests.containsAll(doc.rcStrain));
        assertTrue(doc.suggests.containsAll(doc.proteinNames));
        Set<String> gene4More =
                doc.geneNamesExact.stream()
                        .filter(gn -> gn.length() >= 4)
                        .collect(Collectors.toSet());
        assertTrue(doc.suggests.containsAll(gene4More));
    }

    @Test
    void testConvertIsoformEntry() throws Exception {
        String file = "Q9EPI6-2.sp";
        UniProtKBEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-2", doc.accession);
        assertEquals("Q9EPI6", doc.canonicalAccession);
        assertEquals(1, doc.id.size());
        assertTrue(doc.id.contains("NSMF-2_RAT"));
        assertTrue(doc.isIsoform);
        assertTrue(doc.reviewed);

        assertEquals(5, doc.proteinNames.size());
        assertTrue(
                doc.proteinNames.contains(
                        "Isoform 2 of NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(
                doc.proteinNames.contains(
                        "Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(
                doc.proteinNames.contains(
                        "Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("Isoform 2 of NMDA receptor syn", doc.proteinsNamesSort);

        assertEquals(0, doc.ecNumbers.size());
        assertEquals(0, doc.ecNumbersExact.size());

        assertEquals("20-JUN-2018", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(4, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(10116, doc.organismTaxId);

        assertEquals(0, doc.encodedIn.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(108, doc.crossRefs.size());
        assertTrue(doc.crossRefs.contains("embl-CAC20867.1"));
        assertTrue(doc.crossRefs.contains("embl-CAC20867"));
        assertTrue(doc.crossRefs.contains("CAC20867.1"));
        assertTrue(doc.crossRefs.contains("CAC20867"));

        assertEquals(2, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("embl"));

        assertEquals(8, doc.referenceTitles.size());
        assertTrue(
                doc.referenceTitles.contains(
                        "Characterization of the novel brain-specific protein Jacob."));

        assertEquals(63, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R."));
        assertTrue(doc.referenceAuthors.contains("Wray S."));
        assertTrue(doc.referenceAuthors.contains("The MGC Project Team"));

        assertEquals(8, doc.referenceCitationIds.size());
        assertTrue(doc.referenceCitationIds.contains("CI-ASPSN3R5FFN1I"));
        assertTrue(doc.referenceCitationIds.contains("15489334"));

        assertEquals(6, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(8, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(d1Aug2000));

        assertEquals(6, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(1, doc.proteinsWith.size());
        assertTrue(doc.proteinsWith.contains(ProteinsWith.ALTERNATIVE_PRODUCTS.getValue()));

        assertEquals(1, doc.commentMap.keySet().size());
        assertEquals(1, doc.commentMap.size());
        assertTrue(doc.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));

        assertEquals(0, doc.featuresMap.size());

        assertEquals(1, doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(0, doc.subcellLocationTerm.size());
        assertEquals(0, doc.subcellLocationNote.size());

        assertEquals(4, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertTrue(doc.ap.contains("described"));
        assertEquals(3, doc.apAs.size());
        assertTrue(doc.apAs.contains("No experimental confirmation available."));
        assertTrue(doc.apAs.contains("external"));

        assertEquals(0, doc.interactors.size());

        assertEquals(0, doc.familyInfo.size());

        assertEquals(57478, doc.seqMass);
        assertEquals(509, doc.seqLength);

        assertEquals(2, doc.rcTissue.size());
        assertTrue(doc.rcTissue.contains("Hippocampus"));

        assertEquals(2, doc.rcStrain.size());
        assertTrue(doc.rcStrain.contains("Wistar"));

        assertEquals(13, doc.scopes.size());
        assertTrue(doc.scopes.contains("SUBCELLULAR LOCATION"));

        assertEquals(50, doc.goes.size());
        assertTrue(doc.goes.contains("0030863"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
        assertFalse(doc.evidenceExperimental);
    }

    @Test
    void testConvertIsoformCanonical() throws Exception {
        String file = "Q9EPI6-1.sp";
        UniProtKBEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-1", doc.accession);
        assertNull(doc.canonicalAccession);
        assertNull(doc.isIsoform);
        assertNull(doc.reviewed);
    }

    private UniProtKBEntry parse(String file) throws Exception {
        InputStream is =
                UniProtKBEntryConverterIT.class
                        .getClassLoader()
                        .getResourceAsStream("2020_02/uniprotkb/" + file);
        assertNotNull(is);
        SupportingDataMap supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        "2020_02/keyword/keywlist.txt",
                        "2020_02/disease/humdisease.txt",
                        "2020_02/subcell/subcell.txt",
                        null);
        DefaultUniProtParser parser = new DefaultUniProtParser(supportingDataMap, false);
        return parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
    }

    private UniProtDocument convertEntry(UniProtKBEntry entry) {
        return converter.convert(entry);
    }
}

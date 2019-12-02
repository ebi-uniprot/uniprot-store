package indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
class UniProtEntryConverterIT {
    private static final String CC_CATALYTIC_ACTIVITY = "cc_catalytic_activity";
    private static final String CC_ALTERNATIVE_PRODUCTS_FIELD = "cc_alternative_products";
    private static final String CC_SIMILARITY_FIELD = "cc_similarity";
    private static final String CCEV_SIMILARITY_FIELD = "ccev_similarity";
    private static final String FT_CONFLICT_FIELD = "ft_conflict";
    private static final String FTEV_CONFLICT_FIELD = "ftev_conflict";
    private static final String FTLEN_CHAIN_FIELD = "ftlen_chain";
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
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);
        assertEquals("A0PHU1", doc.accession);
        assertEquals("A0PHU1_9CICH", doc.id);
        assertFalse(doc.reviewed);
        assertEquals(1, doc.proteinNames.size());
        assertEquals("Cytochrome b", doc.proteinNames.get(0));
        assertEquals("Cytochrome b", doc.proteinsNamesSort);
        assertEquals(0, doc.ecNumbers.size());

        assertEquals("09-JAN-2007", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("07-JAN-2015", dateFormat.format(doc.lastModified).toUpperCase());

        assertEquals(24, doc.keywords.size());
        assertEquals("KW-0249", doc.keywords.get(0));
        assertEquals("Electron transport", doc.keywords.get(1));

        assertEquals(172543, doc.organismTaxId);

        assertEquals(1, doc.organelles.size());
        assertEquals("mitochondrion", doc.organelles.get(0));

        assertEquals(1, doc.organismHostIds.size());
        assertEquals(9539, doc.organismHostIds.get(0).intValue());

        assertEquals(52, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("embl-AAY21541.1"));
        assertTrue(doc.xrefs.contains("embl-AAY21541"));
        assertTrue(doc.xrefs.contains("AAY21541.1"));
        assertTrue(doc.xrefs.contains("AAY21541"));

        assertEquals(8, doc.databases.size());
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

        assertEquals(3, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SIMILARITY_FIELD));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("ECO_0000256"));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("automatic"));

        assertEquals("HOMOLOGY", doc.proteinExistence);
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

        assertEquals(3, doc.cofactorChebi.size());
        assertTrue(doc.cofactorChebi.contains("heme"));

        assertEquals(1, doc.cofactorNote.size());
        assertTrue(doc.cofactorNote.contains("Binds 2 heme groups non-covalently."));

        assertEquals(2, doc.cofactorChebiEv.size());
        assertTrue(doc.cofactorChebiEv.contains("ECO_0000256"));

        assertEquals(2, doc.cofactorNoteEv.size());
        assertTrue(doc.cofactorNoteEv.contains("ECO_0000256"));

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
    }

    @Test
    void testConvertFullQ9EPI6Entry() throws Exception {
        String file = "Q9EPI6.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6", doc.accession);
        assertEquals(5, doc.secacc.size());
        assertEquals("Q7TSC6", doc.secacc.get(1));
        assertEquals("NSMF_RAT", doc.id);
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

        assertEquals(1, doc.ecNumbers.size());
        assertEquals(1, doc.ecNumbersExact.size());

        assertEquals("29-OCT-2014", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(36, doc.keywords.size());
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

        assertEquals(0, doc.organelles.size());
        assertEquals(0, doc.organismHostNames.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(153, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("refseq-NM_001270626.1"));
        assertTrue(doc.xrefs.contains("refseq-NM_001270626"));
        assertTrue(doc.xrefs.contains("NM_001270626.1"));
        assertTrue(doc.xrefs.contains("NM_001270626"));

        assertEquals(21, doc.databases.size());
        assertTrue(doc.databases.contains("refseq"));
        assertTrue(doc.databases.contains("ensembl"));

        assertEquals(7, doc.referenceTitles.size());
        assertTrue(
                doc.referenceTitles.contains(
                        "Characterization of the novel brain-specific protein Jacob."));

        assertEquals(55, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R."));
        assertTrue(doc.referenceAuthors.contains("Wray S."));

        assertEquals(5, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(1, doc.referenceOrganizations.size());
        assertTrue(doc.referenceOrganizations.contains("The MGC Project Team"));

        assertEquals(7, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(new Date(965084400000L)));

        assertEquals(5, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(16, doc.proteinsWith.size());
        assertTrue(doc.proteinsWith.contains("chain"));
        assertFalse(doc.proteinsWith.contains("similarity")); // filtered out
        assertFalse(doc.proteinsWith.contains("conflict")); // filtered out

        assertEquals(10, doc.commentMap.keySet().size());
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

        assertEquals(10, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SIMILARITY_FIELD));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("ECO_0000305"));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("manual"));

        assertEquals(9, doc.featuresMap.size());
        assertTrue(doc.featuresMap.containsKey(FT_CONFLICT_FIELD));
        assertTrue(doc.featuresMap.get(FT_CONFLICT_FIELD).contains("in Ref. 3; AAH87719"));

        assertEquals(9, doc.featureEvidenceMap.size());
        assertTrue(doc.featureEvidenceMap.containsKey(FTEV_CONFLICT_FIELD));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("ECO_0000305"));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("manual"));

        assertEquals(9, doc.featureLengthMap.size());
        assertTrue(doc.featureLengthMap.containsKey(FTLEN_CHAIN_FIELD));
        assertTrue(doc.featureLengthMap.get(FTLEN_CHAIN_FIELD).contains(531));

        assertEquals("PROTEIN_LEVEL", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertTrue(doc.commentMap.containsKey(CC_CATALYTIC_ACTIVITY));
        assertTrue(doc.commentMap.get(CC_CATALYTIC_ACTIVITY).contains("CHEBI:16526"));
        assertTrue(doc.commentMap.get(CC_CATALYTIC_ACTIVITY).contains("RHEA:10732"));

        assertEquals(26, doc.subcellLocationTerm.size());
        assertTrue(doc.subcellLocationTerm.contains("Nucleus envelope"));
        assertEquals(0, doc.subcellLocationTermEv.size());
        assertEquals(1, doc.subcellLocationNote.size());
        assertEquals(2, doc.subcellLocationNoteEv.size());
        assertTrue(doc.subcellLocationNoteEv.contains("ECO_0000250"));
        assertTrue(doc.subcellLocationNoteEv.contains("manual"));
        List<String> subcellTerm =
                doc.subcellLocationTerm.stream()
                        .filter(val -> !val.startsWith("SL-"))
                        .collect(Collectors.toList());

        assertEquals(3, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertEquals(2, doc.apAs.size());
        assertTrue(doc.apAs.contains("Additional isoforms seem to exist."));

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
    }

    @Test
    void testConvertIsoformEntry() throws Exception {
        String file = "Q9EPI6-2.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-2", doc.accession);
        assertEquals(1, doc.secacc.size());
        assertEquals("Q9EPI6", doc.secacc.get(0));
        assertEquals("NSMF-2_RAT", doc.id);
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

        assertEquals(36, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(10116, doc.organismTaxId);

        assertEquals(0, doc.organelles.size());
        assertEquals(0, doc.organismHostIds.size());

        assertEquals(56, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("embl-CAC20867.1"));
        assertTrue(doc.xrefs.contains("embl-CAC20867"));
        assertTrue(doc.xrefs.contains("CAC20867.1"));
        assertTrue(doc.xrefs.contains("CAC20867"));

        assertEquals(2, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("embl"));

        assertEquals(8, doc.referenceTitles.size());
        assertTrue(
                doc.referenceTitles.contains(
                        "Characterization of the novel brain-specific protein Jacob."));

        assertEquals(62, doc.referenceAuthors.size());
        assertTrue(doc.referenceAuthors.contains("Kramer P.R."));
        assertTrue(doc.referenceAuthors.contains("Wray S."));

        assertEquals(6, doc.referencePubmeds.size());
        assertTrue(doc.referencePubmeds.contains("15489334"));

        assertEquals(1, doc.referenceOrganizations.size());
        assertTrue(doc.referenceOrganizations.contains("The MGC Project Team"));

        assertEquals(8, doc.referenceDates.size());
        assertTrue(doc.referenceDates.contains(new Date(965084400000L)));

        assertEquals(6, doc.referenceJournals.size());
        assertTrue(doc.referenceJournals.contains("Genome Res."));

        assertEquals(1, doc.proteinsWith.size());
        assertTrue(doc.proteinsWith.contains("alternative_products"));

        assertEquals(1, doc.commentMap.keySet().size());
        assertEquals(1, doc.commentMap.size());
        assertTrue(doc.commentMap.containsKey(CC_ALTERNATIVE_PRODUCTS_FIELD));

        assertEquals(1, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey("ccev_alternative_products"));

        assertEquals(0, doc.featuresMap.size());
        assertEquals(0, doc.featureEvidenceMap.size());
        assertEquals(0, doc.featureLengthMap.size());

        assertEquals("PROTEIN_LEVEL", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertEquals(0, doc.subcellLocationTerm.size());
        assertEquals(0, doc.subcellLocationTermEv.size());
        assertEquals(0, doc.subcellLocationNote.size());
        assertEquals(0, doc.subcellLocationNoteEv.size());

        assertEquals(2, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertEquals(1, doc.apAs.size());
        assertTrue(doc.apAs.contains("No experimental confirmation available."));

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

        //        assertEquals(50, doc.defaultGo.size());
        //        assertTrue(doc.defaultGo.contains("membrane"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
        //        assertNotNull(doc.avro_binary);
    }

    @Test
    void testConvertIsoformCanonical() throws Exception {
        String file = "Q9EPI6-1.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-1", doc.accession);
        assertNull(doc.isIsoform);
        assertNull(doc.reviewed);
    }

    private UniProtEntry parse(String file) throws Exception {
        InputStream is =
                UniProtEntryConverterIT.class
                        .getClassLoader()
                        .getResourceAsStream("uniprotkb/" + file);
        assertNotNull(is);
        SupportingDataMap supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        "keyword/keywlist.txt",
                        "disease/humdisease.txt",
                        "subcell/subcell.txt",
                        null);
        DefaultUniProtParser parser = new DefaultUniProtParser(supportingDataMap, false);
        return parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
    }

    private UniProtDocument convertEntry(UniProtEntry entry) {
        return converter.convert(entry);
    }
}

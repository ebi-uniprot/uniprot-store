package org.uniprot.store.indexer.uniprotkb.converter;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.Chebi;
import org.uniprot.core.cv.chebi.ChebiBuilder;
import org.uniprot.core.cv.chebi.ChebiRepo;
import org.uniprot.core.cv.ec.ECBuilder;
import org.uniprot.core.cv.ec.ECRepo;
import org.uniprot.core.cv.taxonomy.TaxonomicNode;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTerm;
import org.uniprot.store.indexer.uniprot.go.GoTermFileReader;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;
import org.uniprot.store.indexer.uniprotkb.processor.UniProtEntryDocumentPairProcessor;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.uniprot.core.util.Utils.nonNull;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;
import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.createSuggestionMapKey;

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

    private TaxonomyRepo repoMock;
    private GoRelationRepo goRelationRepoMock;
    private ChebiRepo chebiRepoMock;
    private HashMap<String, SuggestDocument> suggestions;
    private ECRepo ecRepoMock;

    @BeforeEach
    void setUp() {
        repoMock = mock(TaxonomyRepo.class);
        goRelationRepoMock = mock(GoRelationRepo.class);
        chebiRepoMock = mock(ChebiRepo.class);
        ecRepoMock = mock(ECRepo.class);
        suggestions = new HashMap<>();
        converter = new UniProtEntryConverter(repoMock, goRelationRepoMock, mock(PathwayRepo.class), chebiRepoMock,
                                              ecRepoMock, suggestions);
        dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
    }

    @Test
    void testConvertFullA0PHU1Entry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(172543, "Cichlasoma festae", null, null, null));
       Set<GoTerm> ancestors = new HashSet<>();
        ancestors.addAll(getMockParentGoTerm());
        ancestors.addAll(getMockPartOfGoTerm());
        when(goRelationRepoMock.getAncestors("GO:0016021", asList(IS_A, PART_OF))).thenReturn(ancestors);
        String file = "A0PHU1.txl";
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

        assertEquals(1, doc.organismName.size());
        assertEquals("Cichlasoma festae", doc.organismName.get(0));
        assertEquals("Cichlasoma festae", doc.organismSort);
        assertEquals(172543, doc.organismTaxId);
        assertNull(doc.popularOrganism);
        assertEquals("Cichlasoma festae", doc.otherOrganism);

        assertEquals(1, doc.organismTaxon.size());
        assertEquals("Cichlasoma festae", doc.organismTaxon.get(0));

        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(172543L, doc.taxLineageIds.get(0).longValue());

        assertEquals(1, doc.organelles.size());
        assertEquals("Mitochondrion", doc.organelles.get(0));

        assertEquals(1, doc.organismHostIds.size());
        assertEquals(9539, doc.organismHostIds.get(0).intValue());

        assertEquals(1, doc.organismHostNames.size());
        assertEquals("Cichlasoma festae", doc.organismHostNames.get(0));

        assertEquals(52, doc.xrefs.size());
        assertTrue(doc.xrefs.contains("embl-AAY21541.1"));
        assertTrue(doc.xrefs.contains("embl-AAY21541"));
        assertTrue(doc.xrefs.contains("AAY21541.1"));
        assertTrue(doc.xrefs.contains("AAY21541"));

        assertEquals(8, doc.databases.size());
        assertTrue(doc.databases.contains("go"));
        assertTrue(doc.databases.contains("interpro"));


        assertEquals(1, doc.referenceTitles.size());
        assertTrue(doc.referenceTitles.get(0).startsWith("Phylogeny and biogeography of 91 species of heroine"));

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
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the cytochrome b family."));

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
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the cytochrome b family."));

        assertEquals(2, doc.cofactorChebi.size());
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

        assertEquals(22, doc.goes.size());
        assertTrue(doc.goes.contains("Go term 5"));

        assertEquals(2, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(2, doc.score);

        assertFalse(doc.isIsoform);
    }

    @Test
    void testConvertFullQ9EPI6Entry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(10116, "Rattus norvegicus", "Rat", null, null));
        Chebi chebiId1 = new ChebiBuilder().id("15379").name("Chebi Name 15379").inchiKey("inchikey 15379").build();
        Chebi chebiId2 = new ChebiBuilder().id("16526").name("Chebi Name 16526").build();
        when(chebiRepoMock.getById("15379")).thenReturn(chebiId1);
        when(chebiRepoMock.getById("16526")).thenReturn(chebiId2);
        when(ecRepoMock.getEC("2.7.10.2"))
                .thenReturn(Optional.of(new ECBuilder().id("2.7.10.2").label("EC 1").build()));

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
        assertTrue(doc.proteinNames.contains("NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(doc.proteinNames.contains("Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("NMDA receptor synaptonuclear s", doc.proteinsNamesSort);

        assertEquals(1, doc.ecNumbers.size());
        assertEquals(1, doc.ecNumbersExact.size());
        checkSuggestionsContain(SuggestDictionary.EC, doc.ecNumbersExact, false);

        assertEquals("29-OCT-2014", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(38, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));
        List<String> keywordIds=
        doc.keywords.stream().filter(val ->val.startsWith("KW-"))
        .collect(Collectors.toList());
        checkSuggestionsContain(SuggestDictionary.KEYWORD, keywordIds, false);

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(2, doc.organismName.size());
        assertEquals("Rat", doc.organismName.get(1));
        assertEquals("Rattus norvegicus Rat", doc.organismSort);
        assertEquals(10116, doc.organismTaxId);
        assertEquals("Rat", doc.popularOrganism);
        assertNull(doc.otherOrganism);
        assertEquals(2, doc.organismTaxon.size());
        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(10116L, doc.taxLineageIds.get(0).longValue());
        checkSuggestionsContain(SuggestDictionary.TAXONOMY,
                                doc.taxLineageIds.stream()
                                        .map(Object::toString)
                                        .collect(Collectors.toList()), false);

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
        assertTrue(doc.referenceTitles.contains("Characterization of the novel brain-specific protein Jacob."));

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

        assertEquals(15, doc.proteinsWith.size());
        assertTrue(doc.proteinsWith.contains("chain"));
        assertFalse(doc.proteinsWith.contains("similarity")); //filtered out
        assertFalse(doc.proteinsWith.contains("conflict")); //filtered out

        assertEquals(10, doc.commentMap.keySet().size());
        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the NSMF family."));

        assertTrue(doc.commentMap.containsKey(CC_SIMILARITY_FIELD));
        assertTrue(doc.commentMap.get(CC_SIMILARITY_FIELD).
                contains("SIMILARITY: Belongs to the NSMF family."));

        assertEquals(10, doc.commentEvMap.size());
        assertTrue(doc.commentEvMap.containsKey(CCEV_SIMILARITY_FIELD));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("ECO_0000305"));
        assertTrue(doc.commentEvMap.get(CCEV_SIMILARITY_FIELD).contains("manual"));

        assertEquals(8, doc.featuresMap.size());
        assertTrue(doc.featuresMap.containsKey(FT_CONFLICT_FIELD));
        assertTrue(doc.featuresMap.get(FT_CONFLICT_FIELD).contains("in Ref. 3; AAH87719"));

        assertEquals(8, doc.featureEvidenceMap.size());
        assertTrue(doc.featureEvidenceMap.containsKey(FTEV_CONFLICT_FIELD));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("ECO_0000305"));
        assertTrue(doc.featureEvidenceMap.get(FTEV_CONFLICT_FIELD).contains("manual"));

        assertEquals(8, doc.featureLengthMap.size());
        assertTrue(doc.featureLengthMap.containsKey(FTLEN_CHAIN_FIELD));
        assertTrue(doc.featureLengthMap.get(FTLEN_CHAIN_FIELD).contains(531));

        assertEquals("PROTEIN_LEVEL", doc.proteinExistence);
        assertFalse(doc.fragment);
        assertFalse(doc.precursor);
        assertTrue(doc.active);
        assertFalse(doc.d3structure);

        assertTrue(doc.commentMap.containsKey(CC_CATALYTIC_ACTIVITY));
        assertThat(doc.commentMap.get(CC_CATALYTIC_ACTIVITY), hasItems(
                containsString(chebiId1.getId()), containsString(chebiId2.getId())));
        checkCatalyticChebiSuggestions(asList(chebiId1, chebiId2));

        assertEquals(26, doc.subcellLocationTerm.size());
        assertTrue(doc.subcellLocationTerm.contains("Nucleus envelope"));
        assertEquals(0, doc.subcellLocationTermEv.size());
        assertEquals(1, doc.subcellLocationNote.size());
        assertEquals(2, doc.subcellLocationNoteEv.size());
        assertTrue(doc.subcellLocationNoteEv.contains("ECO_0000250"));
        assertTrue(doc.subcellLocationNoteEv.contains("manual"));
        List<String> subcellTerm=
        doc.subcellLocationTerm.stream().filter(val-> !val.startsWith("SL-"))
        .collect(Collectors.toList());
        checkSuggestionsContain(SuggestDictionary.SUBCELL, subcellTerm, true);

        assertEquals(2, doc.ap.size());
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
        checkSuggestionsContain(SuggestDictionary.GO, doc.goIds, false);

//        assertEquals(50, doc.defaultGo.size());
//        assertTrue(doc.defaultGo.contains("membrane"));

        assertEquals(4, doc.goWithEvidenceMaps.size());
        assertTrue(doc.goWithEvidenceMaps.containsKey("go_ida"));

        assertEquals(5, doc.score);
//        assertNotNull(doc.avro_binary);

        assertFalse(doc.isIsoform);
    }

    @Test
    void testConvertIsoformEntry() throws Exception {
        when(repoMock.retrieveNodeUsingTaxID(anyInt()))
                .thenReturn(getTaxonomyNode(10116, "Rattus norvegicus", "Rat", null, null));

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
        assertTrue(doc.proteinNames
                           .contains("Isoform 2 of NMDA receptor synaptonuclear signaling and neuronal migration factor"));
        assertTrue(doc.proteinNames.contains("Juxtasynaptic attractor of caldendrin on dendritic boutons protein"));
        assertTrue(doc.proteinNames.contains("Jacob protein"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic luteinizing hormone-releasing hormone factor"));
        assertTrue(doc.proteinNames.contains("Nasal embryonic LHRH factor"));
        assertEquals("Isoform 2 of NMDA receptor syn", doc.proteinsNamesSort);

        assertEquals(0, doc.ecNumbers.size());
        assertEquals(0, doc.ecNumbersExact.size());

        assertEquals("20-JUN-2018", dateFormat.format(doc.lastModified).toUpperCase());
        assertEquals("19-JUL-2005", dateFormat.format(doc.firstCreated).toUpperCase());
        assertEquals("01-MAR-2001", dateFormat.format(doc.sequenceUpdated).toUpperCase());

        assertEquals(38, doc.keywords.size());
        assertEquals("KW-0025", doc.keywords.get(0));
        assertEquals("Alternative splicing", doc.keywords.get(1));

        assertEquals(3, doc.geneNames.size());
        assertEquals("Nsmf", doc.geneNames.get(0));
        assertEquals("Nsmf Jac Nelf", doc.geneNamesSort);
        assertEquals(3, doc.geneNamesExact.size());

        assertEquals(2, doc.organismName.size());
        assertEquals("Rat", doc.organismName.get(1));
        assertEquals("Rattus norvegicus Rat", doc.organismSort);
        assertEquals(10116, doc.organismTaxId);
        assertEquals("Rat", doc.popularOrganism);
        assertNull(doc.otherOrganism);
        assertEquals(2, doc.organismTaxon.size());
        assertEquals(1, doc.taxLineageIds.size());
        assertEquals(10116L, doc.taxLineageIds.get(0).longValue());

        assertEquals(0, doc.organelles.size());
        assertEquals(0, doc.organismHostNames.size());
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
        assertTrue(doc.referenceTitles.contains("Characterization of the novel brain-specific protein Jacob."));

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


        assertEquals(1, doc.ap.size());
        assertTrue(doc.ap.contains("Alternative splicing"));
        assertEquals(1, doc.apAs.size());
        assertTrue(doc.apAs.contains("Alternative splicing"));

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
        when(repoMock.retrieveNodeUsingTaxID(anyInt())).thenReturn(Optional.<TaxonomicNode>empty());

        String file = "Q9EPI6-1.sp";
        UniProtEntry entry = parse(file);
        assertNotNull(entry);
        UniProtDocument doc = convertEntry(entry);
        assertNotNull(doc);

        assertEquals("Q9EPI6-1", doc.accession);
        assertNull(doc.isIsoform);
        assertNull(doc.reviewed);
    }

    private void checkCatalyticChebiSuggestions(List<Chebi> chebiList) {
        for (Chebi chebi : chebiList) {
            String id = "CHEBI:" + chebi.getId();
            SuggestDocument chebiDoc = suggestions
                    .get(createSuggestionMapKey(SuggestDictionary.CATALYTIC_ACTIVITY, id));
            assertThat(chebiDoc.id, is(id));
            assertThat(chebiDoc.value, is(chebi.getName()));
            if (nonNull(chebi.getInchiKey())) {
                assertThat(chebiDoc.altValues, contains(chebi.getInchiKey()));
            }
        }
    }

    private void checkSuggestionsContain(SuggestDictionary dict, Collection<String> values, boolean sizeOnly) {
        // the number of suggestions for this dictionary is the same size as values
        List<String> foundSuggestions = this.suggestions.keySet().stream()
                .filter(key -> key.startsWith(dict.name()))
                .collect(Collectors.toList());
        assertThat(values, hasSize(foundSuggestions.size()));

        if (!sizeOnly) {
            // values are a subset of the suggestions for this dictionary
            for (String value : values) {
                String key = createSuggestionMapKey(dict, value);
                assertTrue(this.suggestions.containsKey(key));
                SuggestDocument document = this.suggestions.get(key);
                assertThat(document.value, is(not(nullValue())));
            }
        }
    }

    private UniProtEntry parse(String file) throws Exception {
        InputStream is = UniProtEntryDocumentPairProcessor.class.getClassLoader()
                .getResourceAsStream("uniprotkb/" + file);
        assertNotNull(is);
        SupportingDataMap supportingDataMap = new SupportingDataMapImpl("uniprotkb/keywlist.txt",
                                                                        "uniprotkb/humdisease.txt",
                                                                        "target/test-classes/uniprotkb/PMID.GO.dr_ext.txt",
                                                                        "uniprotkb/subcell.txt");
        DefaultUniProtParser parser = new DefaultUniProtParser(supportingDataMap, false);
        return parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
    }

    private UniProtDocument convertEntry(UniProtEntry entry) {
        return converter.convert(entry);
    }


    private Set<GoTerm> getMockParentGoTerm() {
        return new HashSet<>(asList(
                new GoTermFileReader.GoTermImpl("GO:123", "Go term 3"),
                new GoTermFileReader.GoTermImpl("GO:124", "Go term 4")
        ));

    }

    private Set<GoTerm> getMockPartOfGoTerm() {
        return new HashSet<>(asList(
                new GoTermFileReader.GoTermImpl("GO:125", "Go term 5"),
                new GoTermFileReader.GoTermImpl("GO:126", "Go term 6")
        ));
    }

    private Optional<TaxonomicNode> getTaxonomyNode(int id, String scientificName, String commonName, String synonym, String mnemonic) {
        return Optional.of(new TaxonomicNode() {
            @Override
            public int id() {
                return id;
            }

            @Override
            public String scientificName() {
                return scientificName;
            }

            @Override
            public String commonName() {
                return commonName;
            }

            @Override
            public String synonymName() {
                return synonym;
            }

            @Override
            public String mnemonic() {
                return mnemonic;
            }

            @Override
            public TaxonomicNode parent() {
                return null;
            }

            @Override
            public boolean hasParent() {
                return false;
            }
        });
    }
}
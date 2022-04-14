package org.uniprot.store.indexer.help;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.indexer.help.HelpPageReader.CATEGORIES_COLON;
import static org.uniprot.store.indexer.help.HelpPageReader.TITLE_COLON;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageItemReaderTest {

    @Test
    void testRead() throws Exception {
        HelpPageItemReader reader = new HelpPageItemReader("src/test/resources/help");
        HelpDocument helpDoc;
        while ((helpDoc = reader.read()) != null) {
            verifyHelpDocument(helpDoc);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "my-category|my-category",
                " my-category|my-category",
                " my-category |my-category",
                "my-category |my-category",
                " category1,category2|category1,category2",
                " category1, category2|category1,category2",
                "my-category,|my-category",
                "my-category,,|my-category",
                "|"
            })
    void checkCategories(String testInput) {
        HelpPageReader reader = new HelpPageReader();
        HelpDocument.HelpDocumentBuilder builder = HelpDocument.builder().id("/my/documment");

        String[] testInputArr = testInput.split("\\|");
        String categoryLineValue = "";
        List<String> requiredCategories = null;
        if (testInputArr.length > 0) {
            categoryLineValue = testInputArr[0];
            requiredCategories =
                    Arrays.stream(testInputArr[1].split(",")).collect(Collectors.toList());
        }

        reader.populateMeta(builder, CATEGORIES_COLON + categoryLineValue);

        assertThat(builder.build().getCategories(), CoreMatchers.is(requiredCategories));
    }

    @ParameterizedTest
    @ValueSource(strings = {"title|title", " title|title", " title |title", "title |title", "|"})
    void checkTitles(String testInput) {
        HelpPageReader reader = new HelpPageReader();
        HelpDocument.HelpDocumentBuilder builder = HelpDocument.builder().id("/my/documment");

        String[] testInputArr = testInput.split("\\|");
        String titleLineValue = "";
        String requiredTitle = null;
        if (testInputArr.length > 0) {
            titleLineValue = testInputArr[0];
            requiredTitle = testInputArr[1];
        }

        reader.populateMeta(builder, TITLE_COLON + titleLineValue);

        assertThat(builder.build().getTitle(), CoreMatchers.is(requiredTitle));
    }

    private void verifyHelpDocument(HelpDocument helpDoc) {
        assertNotNull(helpDoc);
        assertNotNull(helpDoc.getId());
        assertNotNull(helpDoc.getDocumentId());
        assertNotNull(helpDoc.getTitle());
        assertNotNull(helpDoc.getContent());
        assertNotNull(helpDoc.getCategories());
        assertFalse(helpDoc.getCategories().isEmpty());
        boolean found =false;
        if ("3d-structure_annotation_in_swiss-prot".equals(helpDoc.getId())) {
            verify3DAnnotation(helpDoc);
            found =true;
        } else if ("about".equals(helpDoc.getId())) {
            verifyAbout(helpDoc);
            found =true;
        }else if ("2019-11-13-release".equals(helpDoc.getId())) {
        	verify20191113release(helpDoc);
        	found=true;
        }
        assertTrue(found);
    }

    private void verify20191113release(HelpDocument helpDoc) {
    	  assertEquals("2019-11-13-release", helpDoc.getDocumentId());
          assertEquals("UniProt release 2019_10", helpDoc.getTitle());
          assertEquals(List.of("mammals", "Toxins"), helpDoc.getCategories());
          assertEquals("releaseNotes", helpDoc.getType());
          assertEquals(LocalDate.parse("2019-11-13"), helpDoc.getReleaseDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
          assertEquals(RELEASE_2019_11_13, helpDoc.getContentOriginal());
	}

	private void verify3DAnnotation(HelpDocument helpDoc) {
        assertEquals("3d-structure_annotation_in_swiss-prot", helpDoc.getDocumentId());
        assertEquals("3D-structure annotation in UniProtKB/Swiss-Prot", helpDoc.getTitle());
        assertEquals(4, helpDoc.getCategories().size());
        assertEquals(
                List.of("3D structure", "Biocuration", "Cross-references", "help"),
                helpDoc.getCategories());
        assertEquals(THREE_D_CONTENT, helpDoc.getContentOriginal());
        assertTrue(
                helpDoc.getContent()
                        .contains("Related keywords: 3D-structure 3D-structure databases"));
        assertEquals(null, helpDoc.getType());
        assertNull(helpDoc.getReleaseDate());
    }

    private void verifyAbout(HelpDocument helpDoc) {
        assertEquals("about", helpDoc.getDocumentId());
        assertEquals("About UniProt", helpDoc.getTitle());
        assertEquals(6, helpDoc.getCategories().size());
        assertEquals("help", helpDoc.getType());
        assertNull(helpDoc.getReleaseDate());
        assertEquals(
                List.of("About UniProt", "Staff", "UniProtKB", "UniRef", "UniParc", "help"),
                helpDoc.getCategories());
        assertEquals(ABOUT_CONTENT, helpDoc.getContentOriginal());

        assertTrue(
                helpDoc.getContent()
                        .contains(
                                "Further information The Universal Protein Resource Printable reference card"));
    }

    static final String THREE_D_CONTENT =
            "\n"
                    + "Manual annotation of entries with 3D-structures has high priority, for the following reasons:\n"
                    + "\n"
                    + "*   3D-structures shed light on protein architecture and provide proof for the existence of a given protein fold.\n"
                    + "*   They show the assembly of multi-protein complexes, and the details of interactions between different proteins, such as an enzyme and its target.\n"
                    + "*   3D-structures yield detailed information about the interactions of a protein with its ligands (substrates, ions, cofactors or regulatory molecules), and so help to elucidate enzyme mechanisms and identify active site residues.\n"
                    + "*   They help to attribute a function to so-far hypothetical proteins.\n"
                    + "*   They show post-translational modifications.\n"
                    + "*   3D-structures pinpoint the exact position of a residue that causes a genetic disease when it is mutated.\n"
                    + "\n"
                    + "UniProtKB/Swiss-Prot document: [Index of PDB cross-references in UniProtKB/Swiss-Prot](http://www.uniprot.org/docs/pdbtosp)  \n"
                    + "  \n"
                    + "Related keywords: [3D-structure](http://www.uniprot.org/keywords/KW%2D0002)  \n"
                    + "  \n"
                    + "[3D-structure databases](http://www.uniprot.org/database/?query=category:%223D+structure+databases%22)\n"
                    + "\n"
                    + "[How can I retrieve all UniProtKB entries for which the 3D structure is known?](http://www.uniprot.org/faq/13)  \n"
                    + "  \n"
                    + "[Why do I find many cross-references to PDB in UniProtKB/Swiss-Prot?](http://www.uniprot.org/faq/2)";

    static final String ABOUT_CONTENT =
            "\n"
                    + "The Universal Protein Resource (UniProt) is a comprehensive resource for protein sequence and annotation data. The UniProt databases are the [UniProt Knowledgebase (UniProtKB)](http://www.uniprot.org/help/uniprotkb), the [UniProt Reference Clusters (UniRef)](http://www.uniprot.org/help/uniref), and the [UniProt Archive (UniParc)](http://www.uniprot.org/help/uniparc). The UniProt consortium and host institutions EMBL-EBI, SIB and PIR are committed to the long-term preservation of the UniProt databases.\n"
                    + "\n"
                    + "![image](http://www.uniprot.org/images/overview.png)\n"
                    + "\n"
                    + "UniProt is a collaboration between the [European Bioinformatics Institute (EMBL-EBI)](https://www.ebi.ac.uk/), the [SIB Swiss Institute of Bioinformatics](https://www.sib.swiss/) and the [Protein Information Resource (PIR)](http://pir.georgetown.edu/). Across the three institutes more than [100 people](http://www.uniprot.org/help/uniprot%5Fstaff) are involved through different tasks such as database curation, software development and support.\n"
                    + "\n"
                    + "EMBL-EBI and SIB together used to produce Swiss-Prot and TrEMBL, while PIR produced the Protein Sequence Database (PIR-PSD). These two data sets coexisted with different protein sequence coverage and annotation priorities. TrEMBL (Translated EMBL Nucleotide Sequence Data Library) was originally created because sequence data was being generated at a pace that exceeded Swiss-Prot's ability to keep up. Meanwhile, PIR maintained the PIR-PSD and related databases, including iProClass, a database of protein sequences and curated families. In 2002 the three institutes decided to pool their resources and expertise and formed the UniProt consortium.\n"
                    + "\n"
                    + "The UniProt consortium is headed by [Alex Bateman](http://www.uniprot.org/bateman), [Alan Bridge](http://www.uniprot.org/help/bridge) and [Cathy Wu](http://pir.georgetown.edu/pirwww/aboutpir/wubio.shtml), supported by [key staff](http://www.uniprot.org/help/key%5Fstaff), and receives valuable input from an independent [Scientific Advisory Board](http://www.uniprot.org/help/sab).\n"
                    + "\n"
                    + "#### Funding\n"
                    + "\n"
                    + "UniProt is supported by the [National Eye Institute (NEI)](https://nei.nih.gov/), [National Human Genome Research Institute (NHGRI)](http://www.genome.gov/), [National Heart, Lung, and Blood Institute (NHLBI)](https://www.nhlbi.nih.gov/), [National Institute on Aging (NIA)](https://www.nia.nih.gov/), [National Institute of Allergy and Infectious Diseases (NIAID)](https://www.niaid.nih.gov/), [National Institute of Diabetes and Digestive and Kidney Diseases (NIDDK)](https://www.niddk.nih.gov/), [National Institute of General Medical Sciences (NIGMS)](http://www.nigms.nih.gov/), [National Institute of Mental Health (NIMH)](https://www.nimh.nih.gov/), and [National Cancer Institute (NCI)](https://www.cancer.gov/) of the [National Institutes of Health (NIH)](http://www.nih.gov/) under grant U24HG007822. Additional support for the EMBL-EBI's involvement in UniProt comes from [European Molecular Biology Laboratory (EMBL)](http://www.embl.org/) core funds, the [British Heart Foundation (BHF)](http://www.bhf.org.uk/) (RG/13/5/30112), the [Parkinson's Disease United Kingdom (PDUK)](http://www.parkinsons.org.uk/) GO grant G-1307, the [Alzheimer's Research UK (ARUK)](https://www.alzheimersresearchuk.org/) grant ARUK-NAS2017A-1, the NIH GO grant U41HG02273 and [Open Targets](https://www.opentargets.org/). UniProt activities at the SIB are additionally supported by the Swiss Federal Government through the [State Secretariat for Education, Research and Innovation SERI](https://www.sbfi.admin.ch/sbfi/en/home.html). PIR's UniProt activities are also supported by the NIH grants R01GM080646, G08LM010720, and P20GM103446, and the [National Science Foundation (NSF)](http://www.nsf.gov/) grant DBI-1062520.\n"
                    + "\n"
                    + "#### Past funding\n"
                    + "\n"
                    + "UniProt has been supported by the NIH grants U01HG02712 (2002-2010) and U41HG006104 (2010-2014).\n"
                    + "\n"
                    + "UniProt activities at EMBL-EBI have benefited from the FP7 SLING project (2009-2012, contract number 226073) and a British Heart Foundation grant (SP/07/007/23671).\n"
                    + "\n"
                    + "UniProt activities at SIB have benefited from EC support through the FP6 FELICS project (2006-2009, contract number 021902), and the FP7 projects SLING (2009-2012, contract number 226073), [GEN2PHEN](http://www.gen2phen.org/) (2009-2013, contract number 200754), and [MICROME](http://www.microme.eu/) (2009-2013, contract number 222886-2).\n"
                    + "\n"
                    + "UniProt activities at PIR have benefited from NIH grants HHSN266200400061C (2004-2009), R01GM080646 (2007-2011), R01GM080646-04S2 (2009-2012), R01GM080646-07S1 (2012), and P20RR016472-09S2 (2009-2011), NSF grants DBI-0138188 (2002-2005) and IIS-0430743 (2004-2007), and UNIDEL Foundation award (2010-2012).\n"
                    + "\n"
                    + "#### Further information\n"
                    + "\n"
                    + "*   [The Universal Protein Resource](http://www.uniprot.org/docs/uniprot%5Fflyer.pdf)\n"
                    + "    \n"
                    + "    Printable reference card for the UniProt databases\n"
                    + "    \n"
                    + "\n"
                    + "*   [How to cite us](http://www.uniprot.org/help/publications)\n"
                    + "\n"
                    + "#### Contact the UniProt consortium members\n"
                    + "\n"
                    + "![image](http://www.uniprot.org/images/embl%2Dlogo.png)  \n"
                    + "  \n"
                    + "[European Bioinformatics Institute (EMBL-EBI)](https://www.ebi.ac.uk/)  \n"
                    + "  \n"
                    + "Wellcome Trust Genome Campus  \n"
                    + "  \n"
                    + "Hinxton Cambridge CB10 1SD  \n"
                    + "  \n"
                    + "United Kingdom  \n"
                    + "  \n"
                    + "Phone: (+44 1223) 494 444  \n"
                    + "  \n"
                    + "Fax: (+44 1223) 494 468\n"
                    + "\n"
                    + "![image](http://www.uniprot.org/images/logos/logo%5Fsib.png)\n"
                    + "\n"
                    + "[SIB Swiss Institute of Bioinformatics](https://www.sib.swiss/)  \n"
                    + "  \n"
                    + "Centre Medical Universitaire  \n"
                    + "  \n"
                    + "1, rue Michel Servet  \n"
                    + "  \n"
                    + "1211 Geneva 4  \n"
                    + "  \n"
                    + "Switzerland  \n"
                    + "  \n"
                    + "Phone: (+41 22) 379 50 50  \n"
                    + "  \n"
                    + "Fax: (+41 22) 379 58 58\n"
                    + "\n"
                    + "![image](http://www.uniprot.org/images/logos/logo%5Fpir.png)\n"
                    + "\n"
                    + "[Protein Information Resource (PIR)](http://pir.georgetown.edu/)  \n"
                    + "  \n"
                    + "Georgetown University Medical Center  \n"
                    + "  \n"
                    + "3300 Whitehaven Street NW  \n"
                    + "  \n"
                    + "Suite 1200  \n"
                    + "  \n"
                    + "Washington, DC 20007  \n"
                    + "  \n"
                    + "United States of America  \n"
                    + "  \n"
                    + "Phone: (+1 202) 687 1039  \n"
                    + "  \n"
                    + "Fax: (+1 202) 687 0057";
    
    static final String RELEASE_2019_11_13 =
    		"\n"
    		+ "# Headline\n"
    		+ "\n"
    		+ "## A scorpion venom toxin may help unravel the mystery of chronic pain\n"
    		+ "\n"
    		+ "The old saying goes 'an ounce of prevention is worth a pound of cure’, and indeed, our body has developed various strategies to alert us of potential dangers to avoid. One contributor to this strategy is [TRPA1](http://www.uniprot.org/uniprot/?query=name%3A%22Transient+receptor+potential+cation+channel+subfamily+A+member+1%22+reviewed%3Ayes), also called the 'wasabi receptor'. TRPA1, a member of the transient receptor family (TRP), is a plasma membrane cation channel expressed by primary afferent sensory neurons. It is activated by chemically reactive electrophiles present in a range of environmental irritants and endogenous inflammatory agents. Cigarette smoke, for example, is rich in reactive electrophiles that can trigger TRPA1 in the cells that line the airways, inducing coughing and sustained airway inflammation. Some plants, such as mustard, wasabi or onions, have evolved compounds that activate TRPA1, possibly to ward off animals that might otherwise eat them. In this context, TRPA1 activation is responsible for the sinus-jolting sting of wasabi and the flood of tears associated with chopping onions.\n"
    		+ "\n"
    		+ "Not only plants produce TRPA1 activating compounds. [Black rock scorpions](http://www.uniprot.org/taxonomy/1330407) do too, as has been reported in [a recent publication by Lin King et al](https://www.ncbi.nlm.nih.gov/pubmed/31447178). This comes as a surprise. Most animal toxins identified so far target voltage-gated ion channels, and the few known to act on TRP channels all activate the capsaicin receptor, TRPV1. The newly discovered black rock scorpion toxin has been called Wasabi receptor toxin or [WaTx](http://www.uniprot.org/uniprot/C0HLG4). In its mature form, it is a 19 amino acid-long peptide, which has the amazing ability to penetrate cells by passive diffusion. This property is not unique to WaTx, other proteins, such as HIV Tat or *Drosophila* penetratin also share it, but WaTx does not have any sequence similarity to them.\n"
    		+ "\n"
    		+ "Once in the cell, WaTx binds TRPA1 at the same site as plant and environmental irritants, but the similarity ends there. Reactive electrophiles covalently bind TRPA1 and produce a large increase in the probability of channel opening characterized by brief transitions between open and closed states. This results in the influx of sodium and calcium ions. The influx of Ca(2+), in turn, causes the exocytosis of dense-core vesicles, the release of calcitonin-gene-related peptide (CGRP) and substance P, and ultimately induces neurogenic inflammation. WaTx non-covalent binding to TRPA1 stabilizes the open state of the channel and prolongs open time. Consequently, it induces neuronal depolarization and subsequent hypersensitivities, which are characteristic of chronic pain. In addition, it decreases the relative Ca(2+)-permeability of the channel. The Ca(+2) influx is not sufficient to trigger CGRP release and does not cause any inflammation. These observations show a striking convergent evolution between plants and animals in terms of binding site, resulting, however, in a very different modulation of cation channel activity and a distinct outcome in terms of inflammation.\n"
    		+ "\n"
    		+ "TRPA1 is expressed in virtually every animal, from worms and humans, but WaTx only activates mammalian orthologs. Why so? It is difficult to say. Black rock scorpions feed on insects like cockroaches and beetles, as well as other small invertebrates such as millipedes, centipedes, spiders and rarely earthworms, but never mammals. Therefore, WaTx may have a deterrent role aimed specifically at mammalian predators.\n"
    		+ "\n"
    		+ "One thing is certain: with WaTx, scorpions provide us with a powerful tool to study the central neural pathways contributing to chronic pain and to investigate the link between chronic pain and inflammation. TRPA1 is emerging as a potential target for new classes of non-opioid analgesics to treat chronic pain.\n"
    		+ "\n"
    		+ "As of this release, [WaTx](http://www.uniprot.org/uniprot/C0HLG4) has been annotated and is painlessly available in UniProtKB/Swiss-Prot.\n"
    		+ "\n"
    		+ "# UniProtKB news\n"
    		+ "\n"
    		+ "## Removal of the cross-references to EcoGene\n"
    		+ "\n"
    		+ "Cross-references to EcoGene have been removed.\n"
    		+ "\n"
    		+ "## Change of the cross-references to DisProt\n"
    		+ "\n"
    		+ "Cross-references to DisProt may now be isoform-specific. The general format of isoform-specific cross-references was described in release [2014\\_03](http://www.uniprot.org/news/2014/03/19/release) .\n"
    		+ "\n"
    		+ "Example: [Q9NQC3](http://www.uniprot.org/uniprot/Q9NQC3)\n"
    		+ "\n"
    		+ "## Changes to the [controlled vocabulary of human diseases](https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/docs/humdisease)\n"
    		+ "\n"
    		+ "New diseases:\n"
    		+ "\n"
    		+ "-   [Deafness, autosomal dominant, 37](http://www.uniprot.org/diseases/DI-05635)\n"
    		+ "-   [Ectodermal dysplasia 15, hypohidrotic/hair type](http://www.uniprot.org/diseases/DI-05636)\n"
    		+ "-   [Epilepsy, rolandic, with proxysmal exercise-induce dystonia and writer's cramp](http://www.uniprot.org/diseases/DI-05646)\n"
    		+ "-   [Epileptic encephalopathy, early infantile, 77](http://www.uniprot.org/diseases/DI-05640)\n"
    		+ "-   [Erythrokeratodermia variabilis et progressiva 6](http://www.uniprot.org/diseases/DI-05634)\n"
    		+ "-   [Hepatitis, fulminant viral](http://www.uniprot.org/diseases/DI-05641)\n"
    		+ "-   [Hyper-IgE recurrent infection syndrome 4, autosomal recessive](http://www.uniprot.org/diseases/DI-05628)\n"
    		+ "-   [Hypoalphalipoproteinemia, primary, 2](http://www.uniprot.org/diseases/DI-05627)\n"
    		+ "-   [Hypopigmentation, organomegaly, and delayed myelination and development](http://www.uniprot.org/diseases/DI-05637)\n"
    		+ "-   [Ichthyotic keratoderma, spasticity, hypomyelination, and dysmorphic facies](http://www.uniprot.org/diseases/DI-05630)\n"
    		+ "-   [Immunodeficiency 64](http://www.uniprot.org/diseases/DI-05632)\n"
    		+ "-   [Microangiopathy and leukoencephalopathy, pontine, autosomal dominant](http://www.uniprot.org/diseases/DI-05644)\n"
    		+ "-   [Mitochondrial DNA depletion syndrome 16, hepatic type](http://www.uniprot.org/diseases/DI-05631)\n"
    		+ "-   [Myopathy, congenital, with tremor](http://www.uniprot.org/diseases/DI-05629)\n"
    		+ "-   [Neurodevelopmental disorder with visual defects and brain anomalies](http://www.uniprot.org/diseases/DI-05639)\n"
    		+ "-   [Night blindness, congenital stationary, 1I](http://www.uniprot.org/diseases/DI-05643)\n"
    		+ "-   [Oculoectodermal syndrome](http://www.uniprot.org/diseases/DI-05645)\n"
    		+ "-   [Oocyte maturation defect 7](http://www.uniprot.org/diseases/DI-05642)\n"
    		+ "-   [Pseudofolliculitis barbae](http://www.uniprot.org/diseases/DI-05647)\n"
    		+ "-   [Robinow syndrome, autosomal recessive 2](http://www.uniprot.org/diseases/DI-05633)\n"
    		+ "-   [Trichothiodystrophy 7, non-photosensitive](http://www.uniprot.org/diseases/DI-05638)\n"
    		+ "-   [Van Esch-O'Driscoll syndrome](http://www.uniprot.org/diseases/DI-05626)\n"
    		+ "\n"
    		+ "## Changes in [subcellular location controlled vocabulary](https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/docs/subcell)\n"
    		+ "\n"
    		+ "New subcellular locations:\n"
    		+ "\n"
    		+ "-   [Neuronal dense core vesicle](http://www.uniprot.org/locations/SL-0526)\n"
    		+ "-   [Neuronal dense core vesicle membrane](http://www.uniprot.org/locations/SL-0532)\n"
    		+ "-   [Postsynaptic early endosome](http://www.uniprot.org/locations/SL-0523)\n"
    		+ "-   [Postsynaptic early endosome membrane](http://www.uniprot.org/locations/SL-0534)\n"
    		+ "-   [Postsynaptic endocytic zone](http://www.uniprot.org/locations/SL-0528)\n"
    		+ "-   [Postsynaptic endosome](http://www.uniprot.org/locations/SL-0522)\n"
    		+ "-   [Postsynaptic Golgi apparatus](http://www.uniprot.org/locations/SL-0521)\n"
    		+ "-   [Postsynaptic recycling endosome](http://www.uniprot.org/locations/SL-0524)\n"
    		+ "-   [Postsynaptic recycling endosome membrane](http://www.uniprot.org/locations/SL-0533)\n"
    		+ "-   [Presynaptic active zone membrane](http://www.uniprot.org/locations/SL-0527)\n"
    		+ "-   [Presynaptic endocytic zone](http://www.uniprot.org/locations/SL-0529)\n"
    		+ "-   [Presynaptic endosome](http://www.uniprot.org/locations/SL-0525)\n"
    		+ "-   [Spine apparatus](http://www.uniprot.org/locations/SL-0530)\n"
    		+ "-   [Synaptic cell membrane](http://www.uniprot.org/locations/SL-0531)";

}

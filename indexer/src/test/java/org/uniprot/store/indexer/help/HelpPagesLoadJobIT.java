package org.uniprot.store.indexer.help;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.uniprot.store.indexer.help.HelpPageItemReaderTest.ABOUT_CONTENT;
import static org.uniprot.store.indexer.help.HelpPageItemReaderTest.THREE_D_CONTENT;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.job.common.listener.ListenerConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.help.HelpDocument;

@ActiveProfiles(profiles = {"job", "offline"})
@ExtendWith(SpringExtension.class)
@SpringBootTest(
        classes = {
            FakeIndexerSpringBootApplication.class,
            SolrTestConfig.class,
            ListenerConfig.class,
            HelpPageIndexStep.class,
            HelpPageIndexJob.class
        })
class HelpPagesLoadJobIT {
    @Autowired private JobLauncherTestUtils jobLauncher;

    @Autowired private UniProtSolrClient solrClient;

    private static final String CLEAN_THREE_D_CONTENT =
            "Manual annotation of entries with 3D-structures has high " +
                    "priority, for the following reasons: 3D-structures " +
                    "shed light on protein architecture and provide proof " +
                    "for the existence of a given protein fold. They show " +
                    "the assembly of multi-protein complexes, and the " +
                    "details of interactions between different proteins, " +
                    "such as an enzyme and its target. 3D-structures yield " +
                    "detailed information about the interactions of a " +
                    "protein with its ligands (substrates, ions, cofactors " +
                    "or regulatory molecules), and so help to elucidate " +
                    "enzyme mechanisms and identify active site residues. " +
                    "They help to attribute a function to so-far hypothetical " +
                    "proteins. They show post-translational modifications. " +
                    "3D-structures pinpoint the exact position of a residue " +
                    "that causes a genetic disease when it is mutated. " +
                    "UniProtKB/Swiss-Prot document: Index of PDB cross-references" +
                    " in UniProtKB/Swiss-Prot Related keywords: 3D-structure " +
                    "3D-structure databases How can I retrieve all UniProtKB " +
                    "entries for which the 3D structure is known? Why do I find " +
                    "many cross-references to PDB in UniProtKB/Swiss-Prot?";

    private static final String CLEAN_ABOUT_CONTENT =
            "The Universal Protein Resource (UniProt) is a comprehensive resource " +
                    "for protein sequence and annotation data. The UniProt databases " +
                    "are the UniProt Knowledgebase (UniProtKB), the UniProt Reference " +
                    "Clusters (UniRef), and the UniProt Archive (UniParc). The UniProt " +
                    "consortium and host institutions EMBL-EBI, SIB and PIR are " +
                    "committed to the long-term preservation of the UniProt databases. " +
                    "UniProt is a collaboration between the European Bioinformatics " +
                    "Institute (EMBL-EBI), the SIB Swiss Institute of Bioinformatics " +
                    "and the Protein Information Resource (PIR). Across the three " +
                    "institutes more than 100 people are involved through different " +
                    "tasks such as database curation, software development and support. " +
                    "EMBL-EBI and SIB together used to produce Swiss-Prot and TrEMBL, " +
                    "while PIR produced the Protein Sequence Database (PIR-PSD). " +
                    "These two data sets coexisted with different protein sequence " +
                    "coverage and annotation priorities. TrEMBL (Translated EMBL " +
                    "Nucleotide Sequence Data Library) was originally created because " +
                    "sequence data was being generated at a pace that exceeded " +
                    "Swiss-Prot's ability to keep up. Meanwhile, PIR maintained the " +
                    "PIR-PSD and related databases, including iProClass, a database " +
                    "of protein sequences and curated families. In 2002 the three " +
                    "institutes decided to pool their resources and expertise and " +
                    "formed the UniProt consortium. The UniProt consortium is headed " +
                    "by Alex Bateman, Alan Bridge and Cathy Wu, supported by key staff, " +
                    "and receives valuable input from an independent Scientific Advisory " +
                    "Board. Funding UniProt is supported by the National Eye Institute " +
                    "(NEI), National Human Genome Research Institute (NHGRI), National " +
                    "Heart, Lung, and Blood Institute (NHLBI), National Institute on " +
                    "Aging (NIA), National Institute of Allergy and Infectious Diseases " +
                    "(NIAID), National Institute of Diabetes and Digestive and Kidney " +
                    "Diseases (NIDDK), National Institute of General Medical Sciences " +
                    "(NIGMS), National Institute of Mental Health (NIMH), and National " +
                    "Cancer Institute (NCI) of the National Institutes of Health (NIH) " +
                    "under grant U24HG007822. Additional support for the EMBL-EBI's " +
                    "involvement in UniProt comes from European Molecular Biology " +
                    "Laboratory (EMBL) core funds, the British Heart Foundation " +
                    "(BHF) (RG/13/5/30112), the Parkinson's Disease United Kingdom " +
                    "(PDUK) GO grant G-1307, the Alzheimer's Research UK (ARUK) " +
                    "grant ARUK-NAS2017A-1, the NIH GO grant U41HG02273 and Open " +
                    "Targets. UniProt activities at the SIB are additionally " +
                    "supported by the Swiss Federal Government through the State " +
                    "Secretariat for Education, Research and Innovation SERI. PIR's " +
                    "UniProt activities are also supported by the NIH grants " +
                    "R01GM080646, G08LM010720, and P20GM103446, and the National " +
                    "Science Foundation (NSF) grant DBI-1062520. Past funding UniProt " +
                    "has been supported by the NIH grants U01HG02712 (2002-2010) and " +
                    "U41HG006104 (2010-2014). UniProt activities at EMBL-EBI have benefited " +
                    "from the FP7 SLING project (2009-2012, contract number 226073) and a " +
                    "British Heart Foundation grant (SP/07/007/23671). UniProt activities at " +
                    "SIB have benefited from EC support through the FP6 FELICS project " +
                    "(2006-2009, contract number 021902), and the FP7 projects SLING (2009-2012, " +
                    "contract number 226073), GEN2PHEN (2009-2013, contract number 200754), and " +
                    "MICROME (2009-2013, contract number 222886-2). UniProt activities at " +
                    "PIR have benefited from NIH grants HHSN266200400061C (2004-2009), " +
                    "R01GM080646 (2007-2011), R01GM080646-04S2 (2009-2012), R01GM080646-07S1 " +
                    "(2012), and P20RR016472-09S2 (2009-2011), NSF grants DBI-0138188 " +
                    "(2002-2005) and IIS-0430743 (2004-2007), and UNIDEL Foundation award " +
                    "(2010-2012). Further information The Universal Protein Resource Printable " +
                    "reference card for the UniProt databases How to cite us Contact the UniProt " +
                    "consortium members European Bioinformatics Institute (EMBL-EBI) Wellcome " +
                    "Trust Genome Campus Hinxton Cambridge CB10 1SD United Kingdom Phone: " +
                    "(+44 1223) 494 444 Fax: (+44 1223) 494 468 SIB Swiss Institute of " +
                    "Bioinformatics Centre Medical Universitaire 1, rue Michel Servet 1211 Geneva " +
                    "4 Switzerland Phone: (+41 22) 379 50 50 Fax: (+41 22) 379 58 58 Protein " +
                    "Information Resource (PIR) Georgetown University Medical Center 3300 " +
                    "Whitehaven Street NW Suite 1200 Washington, DC 20007 United States of " +
                    "America Phone: (+1 202) 687 1039 Fax: (+1 202) 687 0057";

    @Test
    void testHelpCentreLoadJob() throws Exception {
        JobExecution jobExecution = jobLauncher.launchJob();
        assertThat(
                jobExecution.getJobInstance().getJobName(),
                CoreMatchers.is(Constants.HELP_PAGE_INDEX_JOB_NAME));

        BatchStatus status = jobExecution.getStatus();
        assertThat(status, is(BatchStatus.COMPLETED));

        StepExecution indexingStep =
                jobExecution.getStepExecutions().stream()
                        .filter(step -> step.getStepName().equals(Constants.HELP_PAGE_INDEX_STEP))
                        .collect(Collectors.toList())
                        .get(0);

        assertThat(indexingStep.getReadCount(), is(2));
        assertThat(indexingStep.getWriteCount(), is(2));

        List<HelpDocument> response =
                solrClient.query(SolrCollection.help, new SolrQuery("*:*"), HelpDocument.class);
        assertThat(response, is(notNullValue()));
        assertThat(response.size(), is(2));
        HelpDocument doc3D = response.get(0);
        assertThat(doc3D.getId(), is("3d-structure_annotation_in_swiss-prot"));
        assertThat(doc3D.getTitle(), is("3D-structure annotation in UniProtKB/Swiss-Prot"));
        assertThat(
                doc3D.getCategories(),
                containsInAnyOrder("3D structure", "Biocuration", "Cross-references", "help"));
        assertThat(doc3D.getContentOriginal(), is(THREE_D_CONTENT));
        assertThat(doc3D.getContent(), is(CLEAN_THREE_D_CONTENT));
        HelpDocument about = response.get(1);
        assertThat(about.getId(), is("about"));
        assertThat(about.getTitle(), is("About UniProt"));
        assertThat(
                about.getCategories(),
                containsInAnyOrder(
                        "About UniProt", "Staff", "UniProtKB", "UniRef", "UniParc", "help"));
        assertThat(about.getContentOriginal(), is(ABOUT_CONTENT));
        assertThat(about.getContent(), is(CLEAN_ABOUT_CONTENT));
        // clean up
        solrClient.delete(SolrCollection.help, "*:*");
        solrClient.commit(SolrCollection.help);
    }
}

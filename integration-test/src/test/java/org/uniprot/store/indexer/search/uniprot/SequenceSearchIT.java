package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

class SequenceSearchIT {
			private static final String Q6GZX4 = "Q6GZX4";
			private static final String Q6GZX3 = "Q6GZX3";
			private static final String Q6GZY3 = "Q6GZY3";
			private static final String Q197B6 = "Q197B6";
	    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	    private static final String Q196W5 = "Q196W5";
	    private static final String Q6GZN7 = "Q6GZN7";
	    private static final String Q6V4H0 = "Q6V4H0";
	    private static final String P48347 = "P48347";
	    private static final String Q12345 = "Q12345";

	    @RegisterExtension
			static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	    @BeforeAll
	    static void populateIndexWithTestData() throws IOException {
	        // a test entry object that can be modified and added to index
	        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
	        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
	        entryProxy.updateEntryObject(LineType.SQ,
	        		"SQ   SEQUENCE   256 AA;  29735 MW;  B4840739BF7D4121 CRC64;\n" + 
	        		"     MAFSAEDVLK EYDRRRRMEA LLLSLYYPND RKLLDYKEWS PPRVQVECPK APVEWNNPPS\n" + 
	        		"     EKGLIVGHFS GIKYKGEKAQ ASEVDVNKMC CWVSKFKDAM RRYQGIQTCK IPGKVLSDLD\n" + 
	        		"     AKIKAYNLTV EGVEGFVRYS RVTKQHVAAF LKELRHSKQY ENVNLIHYIL TDKRVDIQHL\n" + 
	        		"     EKDLVKDFKA LVESAHRMRQ GHMINVKYIL YQLLKKHGHG PDGPDILTVK TGSKGVLYDD\n" + 
	        		"     SFRKIYTDLG WKFTPL");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
	        entryProxy.updateEntryObject(LineType.SQ,
	                "SQ   SEQUENCE   320 AA;  34642 MW;  9E110808B6E328E0 CRC64;\n" + 
	                "     MSIIGATRLQ NDKSDTYSAG PCYAGGCSAF TPRGTCGKDW DLGEQTCASG FCTSQPLCAR\n" + 
	                "     IKKTQVCGLR YSSKGKDPLV SAEWDSRGAP YVRCTYDADL IDTQAQVDQF VSMFGESPSL\n" + 
	                "     AERYCMRGVK NTAGELVSRV SSDADPAGGW CRKWYSAHRG PDQDAALGSF CIKNPGAADC\n" + 
	                "     KCINRASDPV YQKVKTLHAY PDQCWYVPCA ADVGELKMGT QRDTPTNCPT QVCQIVFNML\n" + 
	                "     DDGSVTMDDV KNTINCDFSK YVPPPPPPKP TPPTPPTPPT PPTPPTPPTP PTPRPVHNRK\n" + 
	                "     VMFFVAGAVL VAILISTVRW");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	     

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
	        entryProxy.updateEntryObject(LineType.SQ,
	                "SQ   SEQUENCE   335 AA;  39427 MW;  170854A423741F50 CRC64;\n" + 
	                "     MPLSVFAEEF AEKSVKRYIG QGLWLPCNLS DYYYYQEFHD EGGYGSIHRV MDKATGNEVI\n" + 
	                "     MKHSYKLDFS PGILPEWWSK FGSLTDDLRE RVVSNHQLRV SREAQILVQA STVLPEMKLH\n" + 
	                "     DYFDDGESFI LIMDYGGRSL ENIASSHKKK ITNLVRYRAY KGNWFYKNWL KQVVDYMIKI\n" + 
	                "     YHKIKILYDI GIYHNDLKPE NVLVDGDHIT IIDFGVADFV PDENERKTWS CYDFRGTIDY\n" + 
	                "     IPPEVGTTGS FDPWHQTVWC FGVMLYFLSF MEYPFHIDNQ FLEYALEGEK LDKLPEPFAQ\n" + 
	                "     LIRECLSVDP DKRPLTSLLD RLTELHHHLQ TIDVW");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q196W5));
	        entryProxy.updateEntryObject(LineType.SQ, 
	        		"SQ   SEQUENCE   363 AA;  40667 MW;  BFE243A6AF604F1A CRC64;\n" + 
	        		"     MSVDSFTSRL AVVMTAVVLV WWAQALPVPS PRRGESDCDA ACRKFLLQYG YLDLGEENCT\n" + 
	        		"     EVDSNRKLCS VDDELVGVPR PLARVDLAAG VSHLQTMAGL EPTGRIDAST ARLFTSPRCG\n" + 
	        		"     VPDVSKYIVA AGRRRRTRRE SVIVCTTRWT TTKSNSNETL VKWWLDQSSM QWLNSTLNWV\n" + 
	        		"     SLTNVLHHSF WKWSKESMLA FQQVSLERDA QIVVRFENGS HGDGWDFDGP GNVLAHAFQP\n" + 
	        		"     GQSLGGDIHL DAAEPWTIYD IDGHDGNSIL HVVLHEIGHA LGLEHSRDPT SIMYAWYTPF\n" + 
	        		"     KYDLGPEDVS AVAGLYGAKP ASSVAAWNPK IQKFYWDRHV RNDLLPLLER DLDAEEEDSD\n" + 
	        		"     EVR");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
	        entryProxy
	                .updateEntryObject(LineType.SQ,
	                		"SQ   SEQUENCE   150 AA;  16507 MW;  AEAAA3A32EFE5EC2 CRC64;\n" + 
	                		"     MHGCNCNRVS GHLSAVRSSG LENGPFGPSG FGPSMWFTMH SGAAERAIRG GYLTENEKAA\n" + 
	                		"     WESWLRNLWV CIPCESCRRH YMGIVNAVDF GSVNTGDKVF RLTVDIHNMV NARLNKPHVT\n" + 
	                		"     LQKAICIYGL DTKLGPASTI TFRANTSTFN");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
	        entryProxy.updateEntryObject(LineType.SQ,
	                "SQ   SEQUENCE   360 AA;  38937 MW;  AB701A2D8921005E CRC64;\n" + 
	                "     MAKSPEVEHP VKAFGWAARD TSGHLSPFHF SRRATGEHDV QFKVLYCGIC HSDLHMIKNE\n" + 
	                "     WGFTKYPIVP GHEIVGIVTE VGSKVEKFKV GDKVGVGCLV GSCRKCDMCT KDLENYCPGQ\n" + 
	                "     ILTYSATYTD GTTTYGGYSD LMVADEHFVI RWPENLPMDI GAPLLCAGIT TYSPLRYFGL\n" + 
	                "     DKPGTHVGVV GLGGLGHVAV KFAKAFGAKV TVISTSESKK QEALEKLGAD SFLVSRDPEQ\n" + 
	                "     MKAAAASLDG IIDTVSAIHP IMPLLSILKS HGKLILVGAP EKPLELPSFP LIAGRKIIAG\n" + 
	                "     SAIGGLKETQ EMIDFAAKHN VLPDVELVSM DYVNTAMERL LKADVKYRFV IDVANTLKSA");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
	        entryProxy.updateEntryObject(LineType.SQ,
	        		"SQ   SEQUENCE   254 AA;  28915 MW;  037405C341845C25 CRC64;\n" + 
	        		"     MENEREKQVY LAKLSEQTER YDEMVEAMKK VAQLDVELTV EERNLVSVGY KNVIGARRAS\n" + 
	        		"     WRILSSIEQK EESKGNDENV KRLKNYRKRV EDELAKVCND ILSVIDKHLI PSSNAVESTV\n" + 
	        		"     FFYKMKGDYY RYLAEFSSGA ERKEAADQSL EAYKAAVAAA ENGLAPTHPV RLGLALNFSV\n" + 
	        		"     FYYEILNSPE SACQLAKQAF DDAIAELDSL NEESYKDSTL IMQLLRDNLT LWTSDLNEEG\n" + 
	        		"     DERTKGADEP QDEN");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
	        entryProxy.updateEntryObject(LineType.SQ, 
	        		"SQ   SEQUENCE   250 AA;  28243 MW;  5A0A2229D6C87ABF CRC64;\n" + 
	        		"     MKFEDLLATN KQVQFAHAAT QHYKSVKTPD FLEKDPHHKK FHNADGLNQQ GSSTPSTATD\n" + 
	        		"     ANAASTASTH TNTTTFKRHI VAVDDISKMN YEMIKNSPGN VITNANQDEI DISTLKTRLY\n" + 
	        		"     KDNLYAMNDN FLQAVNDQIV TLNAAEQDQE TEDPDLSDDE KIDILTKIQE NLLEEYQKLS\n" + 
	        		"     QKERKWFILK ELLLDANVEL DLFSNRGRKA SHPIAFGAVA IPTNVNANSL AFNRTKRRKI\n" + 
	        		"     NKNGLLENIL");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        searchEngine.printIndexContents();
	    }
	    @Test
	    void findSingleByLength() {
	    		String query= query(UniProtField.Search.length, "256");
	    		QueryResponse response = searchEngine.getQueryResponse(query);
	    		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	    		assertThat(retrievedAccessions, hasItem(Q6GZX4));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZX3)));
	    }
	    @Test
	    void findSingleByLengthRange() {
	    		String query= QueryBuilder.rangeQuery(UniProtField.Search.length.name(), "250", "256", true, false);
	    		System.out.println(query);
	    		QueryResponse response = searchEngine.getQueryResponse(query);
	    		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	    		System.out.println(retrievedAccessions);
	    		assertThat(retrievedAccessions,containsInAnyOrder(Q12345, P48347));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	    }
	    @Test
	    void findSingleByMass() {
	    		String query= query(UniProtField.Search.mass, "38937");
	    		QueryResponse response = searchEngine.getQueryResponse(query);
	    		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	    		assertThat(retrievedAccessions, hasItem(Q6V4H0));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZX3)));
	    }
	    @Test
	    void findSingleByMassRange() {
	    		String query= QueryBuilder.rangeQuery(UniProtField.Search.mass.name(), 29734, 39427);
	    		QueryResponse response = searchEngine.getQueryResponse(query);
	    		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	    		assertThat(retrievedAccessions,containsInAnyOrder(Q6GZX4, Q6V4H0, Q6GZX3, Q197B6));
	    		assertThat(retrievedAccessions, not(hasItem(Q12345)));
	    }
}

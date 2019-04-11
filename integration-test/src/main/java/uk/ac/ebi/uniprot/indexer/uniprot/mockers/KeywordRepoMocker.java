package uk.ac.ebi.uniprot.indexer.uniprot.mockers;

import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;

public class KeywordRepoMocker {
	public static KeywordRepo getKeywordRepo() {
	//	 String filePath= KeywordRepoMocker.class.getClassLoader().getResource("keywlist.txt").getFile();		 
		 return new KeywordFileRepo("keywlist.txt");
	}
}

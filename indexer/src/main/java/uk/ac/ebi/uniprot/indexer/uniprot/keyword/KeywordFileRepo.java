package uk.ac.ebi.uniprot.indexer.uniprot.keyword;

import java.util.Optional;

import uk.ac.ebi.uniprot.cv.keyword.KeywordDetail;
import uk.ac.ebi.uniprot.cv.keyword.KeywordService;
import uk.ac.ebi.uniprot.cv.keyword.impl.KeywordServiceImpl;


public class KeywordFileRepo implements KeywordRepo {
	private final KeywordService keywordService;
	

	public KeywordFileRepo(String filename) {
		keywordService = new KeywordServiceImpl(filename);
	}
	@Override
	public Optional<KeywordDetail> getKeyword(String id) {
		return Optional.ofNullable(keywordService.getByAccession(id));
	}

}

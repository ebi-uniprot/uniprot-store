package uk.ac.ebi.uniprot.indexer.uniprot.keyword;

import java.util.Optional;

import uk.ac.ebi.uniprot.cv.keyword.KeywordDetail;

public interface KeywordRepo {
	Optional<KeywordDetail> getKeyword(String id);
}

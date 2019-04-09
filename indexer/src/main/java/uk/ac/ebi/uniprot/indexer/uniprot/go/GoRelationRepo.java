package uk.ac.ebi.uniprot.indexer.uniprot.go;

import java.util.List;

public interface GoRelationRepo {
	List<GoTerm> getIsA(String goId) ;
	List<GoTerm> getPartOf(String goId);
}

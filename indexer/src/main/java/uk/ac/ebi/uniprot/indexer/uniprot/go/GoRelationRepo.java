package uk.ac.ebi.uniprot.indexer.uniprot.go;

import java.util.Set;

public interface GoRelationRepo {
    Set<GoTerm> getIsA(String goId);

    Set<GoTerm> getPartOf(String goId);
}

package uk.ac.ebi.uniprot.indexer.uniprot.pathway;

import uk.ac.ebi.uniprot.cv.pathway.UniPathway;

public interface PathwayRepo {
    UniPathway getFromName(String name);
}

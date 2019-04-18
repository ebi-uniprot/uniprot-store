package uk.ac.ebi.uniprot.indexer.uniprot.mockers;

import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;

public class PathwayRepoMocker {
	public static PathwayRepo getPathwayRepo() {
		return new PathwayFileRepo("unipathway.txt");
	}
}

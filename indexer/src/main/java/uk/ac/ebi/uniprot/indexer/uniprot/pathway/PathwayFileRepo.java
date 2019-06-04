package uk.ac.ebi.uniprot.indexer.uniprot.pathway;


import uk.ac.ebi.uniprot.cv.pathway.UniPathway;
import uk.ac.ebi.uniprot.cv.pathway.UniPathwayService;
import uk.ac.ebi.uniprot.cv.pathway.impl.UniPathwayServiceImpl;

public class PathwayFileRepo implements PathwayRepo {
    private final UniPathwayService unipathwayService;

    public PathwayFileRepo(String filename) {
        unipathwayService = new UniPathwayServiceImpl(filename);
    }

    @Override
    public UniPathway getFromName(String name) {
        return unipathwayService.getByName(name);
    }

}

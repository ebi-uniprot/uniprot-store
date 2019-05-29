package uk.ac.ebi.uniprot.indexer.uniprot.mockers;


import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;

import java.io.File;

public class TaxonomyRepoMocker {

    public static TaxonomyRepo getTaxonomyRepo() {
        String filePath= Thread.currentThread().getContextClassLoader().getResource("taxonomy/taxonomy.dat").getFile();
        File taxonomicFile = new File(filePath);

        FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
        return new TaxonomyMapRepo(taxonomicNodeIterable);
    }

}

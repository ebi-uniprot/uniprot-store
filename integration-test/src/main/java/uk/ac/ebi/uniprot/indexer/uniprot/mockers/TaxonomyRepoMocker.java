package uk.ac.ebi.uniprot.indexer.uniprot.mockers;



import java.io.File;

import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.cv.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyMapRepo;

public class TaxonomyRepoMocker {

    public static TaxonomyRepo getTaxonomyRepo() {
        String filePath= Thread.currentThread().getContextClassLoader().getResource("taxonomy/taxonomy.dat").getFile();
        File taxonomicFile = new File(filePath);

        FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
        return new TaxonomyMapRepo(taxonomicNodeIterable);
    }

}

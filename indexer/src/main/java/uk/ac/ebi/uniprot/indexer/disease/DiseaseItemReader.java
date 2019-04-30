package uk.ac.ebi.uniprot.indexer.disease;

import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.cv.disease.Disease;
import uk.ac.ebi.uniprot.cv.impl.DiseaseFileReader;
import java.io.IOException;
import java.util.Iterator;

public class DiseaseItemReader implements ItemReader<Disease> {
    private Iterator<Disease> diseaseIterator;

    public DiseaseItemReader(String filePath) throws IOException {
        DiseaseFileReader diseaseFileReader = new DiseaseFileReader();
        this.diseaseIterator = diseaseFileReader.getDiseaseIterator(filePath);
    }

    @Override
    public Disease read(){

        if(this.diseaseIterator.hasNext()){
            return this.diseaseIterator.next();
        }

        return null;
    }

}

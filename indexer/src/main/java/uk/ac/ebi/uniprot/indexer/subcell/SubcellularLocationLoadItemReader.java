package uk.ac.ebi.uniprot.indexer.subcell;

import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.uniprot.cv.impl.SubcellularLocationFileReader;
import uk.ac.ebi.uniprot.cv.subcell.SubcellularLocationEntry;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
public class SubcellularLocationLoadItemReader implements ItemReader<SubcellularLocationEntry> {
    private Iterator<SubcellularLocationEntry> subcellularLocationIterator;

    public SubcellularLocationLoadItemReader(String filePath) throws IOException {
        SubcellularLocationFileReader subcellularLocationFileReader = new SubcellularLocationFileReader();
        this.subcellularLocationIterator = subcellularLocationFileReader.parse(filePath).iterator();
    }

    @Override
    public SubcellularLocationEntry read() {
        if (this.subcellularLocationIterator.hasNext()) {
            return this.subcellularLocationIterator.next();
        }
        return null;
    }

}
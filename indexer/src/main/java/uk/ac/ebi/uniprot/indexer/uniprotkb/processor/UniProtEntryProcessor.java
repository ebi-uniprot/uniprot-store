package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.DocumentConversionException;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;

/**
 * // TODO: 18/04/19 genericise this class
 * // TODO: 18/04/19 add test to check if file was created with correct contents
 * Created 10/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryProcessor implements ItemProcessor<UniProtEntryDocumentPair, UniProtEntryDocumentPair> {
    private static final Logger INDEXING_FAILED_LOGGER = LoggerFactory
            .getLogger("indexing-doc-conversion-failed-entries");
    private final UniProtEntryConverter converter;

    public UniProtEntryProcessor(UniProtEntryConverter converter) {
        this.converter = converter;
    }

    @Override
    public UniProtEntryDocumentPair process(UniProtEntryDocumentPair uniProtEntryDocumentPair) {
        UniProtEntry uniProtEntry = uniProtEntryDocumentPair.getEntry();
        try {
            uniProtEntryDocumentPair.setDocument(converter.convert(uniProtEntry));
            return uniProtEntryDocumentPair;
        } catch (DocumentConversionException e) {
            writeFailedEntryToFile(uniProtEntry);
            log.error("Error converting entry: " + uniProtEntry.getPrimaryAccession().getValue(), e);
            return null; // => the item should not be written https://docs.spring.io/spring-batch/trunk/reference/html/domain.html#domainItemProcessor
        }
    }

    private void writeFailedEntryToFile(UniProtEntry uniProtEntry) {
        String entryFF = UniProtFlatfileWriter.write(uniProtEntry);
        INDEXING_FAILED_LOGGER.error(entryFF);
    }
}

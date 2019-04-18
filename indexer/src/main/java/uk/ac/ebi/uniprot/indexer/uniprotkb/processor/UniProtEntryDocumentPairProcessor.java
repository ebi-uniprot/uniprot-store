package uk.ac.ebi.uniprot.indexer.uniprotkb.processor;

import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.processor.EntryDocumentPairProcessor;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairProcessor extends EntryDocumentPairProcessor<UniProtEntry, UniProtDocument, UniProtEntryDocumentPair> {
    public UniProtEntryDocumentPairProcessor(DocumentConverter<UniProtEntry, UniProtDocument> converter) {
        super(converter);
    }

    @Override
    public String extractEntryId(UniProtEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public String entryToString(UniProtEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }
}

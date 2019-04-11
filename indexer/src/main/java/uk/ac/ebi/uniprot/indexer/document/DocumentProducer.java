package uk.ac.ebi.uniprot.indexer.document;

import java.util.List;

import uk.ac.ebi.uniprot.search.document.Document;


/**
 * It represent a conversion (production) from a given source object to the the list of documents it supported.
 * One example of this a UniprotEntryDocumentProducer to produce a UniProtDocument from a UniprotEntry object.
 * <p/>
 * The design  make it possible to have multiple documented produced and then indexed from a single source object.
 * For example, a UniProtEntry source can be used to generate a UniProtDocument or a FeatureDocument. The type of Documents
 * it produces can be configured before indexer run.
 * <p/>
 *
 * @param <S> The type of the source object that is converted into the documents.
 */
public interface DocumentProducer<S> {

    /**
     * Produce all the documents this producer support and constrained by the #configDocumenttypes() method for the given doc.
     * If no configuration found, all supported document is produced.
     *
     * @param doc the source object
     * @return the list of documents produced from the source object.
     */
    public List<Document> produce(S doc);




}

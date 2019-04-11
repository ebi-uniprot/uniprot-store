package uk.ac.ebi.uniprot.indexer.document;



import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.uniprot.indexer.IndexationException;
import uk.ac.ebi.uniprot.search.document.Document;

/**
 * @author wudong
 */
public abstract class AbstractDocumentProducer<S> implements DocumentProducer<S> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

 
    private DocumentConverter<S, ?>[] supportedConverters;

    //this is to set the converter to be used by this producer.
    protected void configConverters(DocumentConverter... converters) {

        Set<DocumentConverter> converterSet = new LinkedHashSet<>();
        for (DocumentConverter converter : converters) {       
                converterSet.add(converter);          
        }

        this.supportedConverters = converterSet.toArray(new DocumentConverter[converterSet.size()]);

        if (this.supportedConverters.length == 0) {
            logger.warn("No converters available to convert data to documents");
        }
    }

    @Override
    public List<Document> produce(S source) {
        if (supportedConverters == null) {
            throw new IndexationException("Converter is not configured yet.");
        }

        ArrayList<Document> result = new ArrayList<>(supportedConverters.length);
        for (DocumentConverter<S, ?> c : supportedConverters) {      
                try {
                    List<? extends Document> convert = c.convert(source);

                    if (convert != null) {
                        result.addAll(convert);
                    }
                } catch (DocumentConversionException e) {
                    logger.error("Conversion error:", e);
                }
         
        }

        return result;
    }

   
}
package org.uniprot.store.search.document.taxonomy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.uniprot.core.taxonomy.*;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.DocumentConverter;

public class TaxonomyInactiveDocumentConverter
        implements DocumentConverter<TaxonomyEntry, TaxonomyDocument> {

    private final ObjectMapper jsonMapper;

    public TaxonomyInactiveDocumentConverter(ObjectMapper objectMapper) {
        jsonMapper = objectMapper;
    }

    @Override
    public TaxonomyDocument convert(TaxonomyEntry inactiveEntry) {
        TaxonomyDocument.TaxonomyDocumentBuilder documentBuilder = TaxonomyDocument.builder();
        documentBuilder.id(String.valueOf(inactiveEntry.getTaxonId()));
        documentBuilder.taxId(inactiveEntry.getTaxonId());
        documentBuilder.active(false);
        documentBuilder.taxonomyObj(getTaxonomyBinary(inactiveEntry));

        return documentBuilder.build();
    }

    private byte[] getTaxonomyBinary(TaxonomyEntry entry) {
        try {
            return jsonMapper.writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new DocumentConversionException(
                    "Unable to parse TaxonomyEntry to binary json: ", e);
        }
    }
}

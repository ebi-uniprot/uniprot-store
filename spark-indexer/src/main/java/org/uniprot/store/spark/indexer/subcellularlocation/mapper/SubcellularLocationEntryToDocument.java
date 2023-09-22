package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.json.parser.subcell.SubcellularLocationJsonConfig;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.spark.indexer.common.exception.IndexHPSDocumentsException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @since 2022-02-3
 */
public class SubcellularLocationEntryToDocument
        implements Serializable, Function<SubcellularLocationEntry, SubcellularLocationDocument> {

    private static final long serialVersionUID = -9175446448727424391L;
    private final ObjectMapper objectMapper;

    public SubcellularLocationEntryToDocument() {
        this.objectMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public SubcellularLocationDocument call(SubcellularLocationEntry entry) throws Exception {
        return convert(entry);
    }

    private SubcellularLocationDocument convert(SubcellularLocationEntry entry) {
        byte[] subcellularLocationByte = getSubcellularLocationEntryBinary(entry);

        return SubcellularLocationDocument.builder()
                .id(entry.getId())
                .name(entry.getName())
                .category(entry.getCategory().getName())
                .definition(entry.getDefinition())
                .synonyms(entry.getSynonyms())
                .subcellularlocationObj(subcellularLocationByte)
                .build();
    }

    private byte[] getSubcellularLocationEntryBinary(SubcellularLocationEntry subcellularLocation) {
        try {
            return this.objectMapper.writeValueAsBytes(subcellularLocation);
        } catch (JsonProcessingException e) {
            throw new IndexHPSDocumentsException(
                    "Unable to parse SubcellularLocationEntry to binary json: ", e);
        }
    }
}

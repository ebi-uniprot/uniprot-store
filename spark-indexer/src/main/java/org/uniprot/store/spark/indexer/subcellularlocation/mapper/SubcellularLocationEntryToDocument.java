package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.json.parser.subcell.SubcellularLocationJsonConfig;
import org.uniprot.store.search.document.subcell.SubcellularLocationDocument;
import org.uniprot.store.spark.indexer.common.exception.IndexHDFSDocumentsException;

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
    Map<String, Statistics> subcellIdStatsMap;
    public SubcellularLocationEntryToDocument(Map<String, Statistics> subcellIdStatsMap) {
        this.objectMapper = SubcellularLocationJsonConfig.getInstance().getFullObjectMapper();
        this.subcellIdStatsMap = subcellIdStatsMap;
    }

    @Override
    public SubcellularLocationDocument call(SubcellularLocationEntry entry) throws Exception {
        return convert(entry);
    }

    private SubcellularLocationDocument convert(SubcellularLocationEntry entry) {
        Statistics statistics = this.subcellIdStatsMap.get(entry.getId());
        SubcellularLocationEntry entryWithStats = SubcellularLocationEntryBuilder.from(entry)
                .statistics(statistics).build();
        byte[] subcellularLocationByte = getSubcellularLocationEntryBinary(entryWithStats);

        return SubcellularLocationDocument.builder()
                .id(entryWithStats.getId())
                .name(entryWithStats.getName())
                .category(entryWithStats.getCategory().getName())
                .definition(entryWithStats.getDefinition())
                .synonyms(entryWithStats.getSynonyms())
                .subcellularlocationObj(subcellularLocationByte)
                .build();
    }

    private byte[] getSubcellularLocationEntryBinary(SubcellularLocationEntry subcellularLocation) {
        try {
            return this.objectMapper.writeValueAsBytes(subcellularLocation);
        } catch (JsonProcessingException e) {
            throw new IndexHDFSDocumentsException(
                    "Unable to parse SubcellularLocationEntry to binary json: ", e);
        }
    }
}

package org.uniprot.store.spark.indexer.taxonomy.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyInactiveReason;
import org.uniprot.core.taxonomy.TaxonomyInactiveReasonType;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyInactiveReasonBuilder;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.document.taxonomy.TaxonomyInactiveDocumentConverter;

import java.io.Serializable;
import java.math.BigDecimal;

public class TaxonomyMergedRowMapper  implements Function<Row, TaxonomyDocument>, Serializable {

    private static final long serialVersionUID = 1078788173920637354L;

    private final ObjectMapper objectMapper;

    public TaxonomyMergedRowMapper() {
        objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument call(Row row) throws Exception {
        TaxonomyInactiveDocumentConverter converter = new TaxonomyInactiveDocumentConverter(objectMapper);
        BigDecimal oldTaxId = row.getDecimal(row.fieldIndex("OLD_TAX_ID"));
        BigDecimal newTaxId = row.getDecimal(row.fieldIndex("NEW_TAX_ID"));
        TaxonomyInactiveReason inactiveReason =
                new TaxonomyInactiveReasonBuilder()
                        .inactiveReasonType(TaxonomyInactiveReasonType.MERGED)
                        .mergedTo(newTaxId.longValue())
                        .build();

        TaxonomyEntry mergedEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(oldTaxId.longValue())
                        .inactiveReason(inactiveReason)
                        .build();

        return converter.convert(mergedEntry);
    }

}

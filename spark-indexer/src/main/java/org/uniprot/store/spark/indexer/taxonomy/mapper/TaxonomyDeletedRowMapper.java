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
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;

public class TaxonomyDeletedRowMapper implements Function<Row, TaxonomyDocument>, Serializable {

    private static final long serialVersionUID = 828695875067609606L;

    private final ObjectMapper objectMapper;

    public TaxonomyDeletedRowMapper() {
        objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument call(Row row) throws Exception {
        TaxonomyInactiveDocumentConverter converter = new TaxonomyInactiveDocumentConverter(objectMapper);
        BigDecimal taxId = row.getDecimal(row.fieldIndex("TAX_ID"));

        TaxonomyInactiveReason inactiveReason =
                new TaxonomyInactiveReasonBuilder()
                        .inactiveReasonType(TaxonomyInactiveReasonType.DELETED)
                        .build();

        TaxonomyEntry deletedEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(taxId.longValue())
                        .inactiveReason(inactiveReason)
                        .build();

        return converter.convert(deletedEntry);
    }

}

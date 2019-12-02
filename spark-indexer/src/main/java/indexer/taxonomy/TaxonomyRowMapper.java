package indexer.taxonomy;

import static indexer.util.RowUtils.hasFieldName;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyRowMapper implements PairFunction<Row, String, TaxonomyEntry>, Serializable {

    private static final long serialVersionUID = -7723532417214033169L;

    @Override
    public Tuple2<String, TaxonomyEntry> call(Row rowValue) throws Exception {
        BigDecimal taxId = rowValue.getDecimal(rowValue.fieldIndex("TAX_ID"));
        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
        builder.taxonId(taxId.longValue());

        if (hasFieldName("SPTR_COMMON", rowValue)) {
            builder.commonName(rowValue.getString(rowValue.fieldIndex("SPTR_COMMON")));
        } else if (hasFieldName("NCBI_COMMON", rowValue)) {
            builder.commonName(rowValue.getString(rowValue.fieldIndex("NCBI_COMMON")));
        }

        if (hasFieldName("SPTR_SCIENTIFIC", rowValue)) {
            builder.scientificName(rowValue.getString(rowValue.fieldIndex("SPTR_SCIENTIFIC")));
        } else if (hasFieldName("NCBI_SCIENTIFIC", rowValue)) {
            builder.scientificName(rowValue.getString(rowValue.fieldIndex("NCBI_SCIENTIFIC")));
        }

        if (hasFieldName("TAX_CODE", rowValue)) {
            builder.mnemonic(rowValue.getString(rowValue.fieldIndex("TAX_CODE")));
        }

        if (hasFieldName("PARENT_ID", rowValue)) {
            builder.parentId(rowValue.getDecimal(rowValue.fieldIndex("PARENT_ID")).longValue());
        }

        if (hasFieldName("RANK", rowValue)) {
            String rank = rowValue.getString(rowValue.fieldIndex("RANK"));
            builder.rank(TaxonomyRank.typeOf(rank));
        } else {
            builder.rank(TaxonomyRank.NO_RANK);
        }

        if (hasFieldName("SPTR_SYNONYM", rowValue)) {
            builder.addSynonyms(rowValue.getString(rowValue.fieldIndex("SPTR_SYNONYM")));
        }

        if (hasFieldName("HIDDEN", rowValue)) {
            builder.hidden(rowValue.getDecimal(rowValue.fieldIndex("HIDDEN")).intValue() == 1);
        }

        builder.active(true);

        return new Tuple2<String, TaxonomyEntry>(String.valueOf(taxId), builder.build());
    }
}

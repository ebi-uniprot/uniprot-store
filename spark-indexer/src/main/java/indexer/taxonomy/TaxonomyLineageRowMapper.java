package indexer.taxonomy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * This class Map SELECT_TAXONOMY_LINEAGE_SQL SQL Row result to a Tuple{key=taxId, value=List of
 * TaxonomyLineage}
 *
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyLineageRowMapper
        implements PairFunction<Row, String, List<TaxonomyLineage>>, Serializable {

    private static final long serialVersionUID = -7723532417214033169L;

    /**
     * @param rowValue TaxonomyLineageReader.SELECT_TAXONOMY_LINEAGE_SQL SQL Row result
     * @return a Tuple{key=taxId, value=List of TaxonomyLineage}
     */
    @Override
    public Tuple2<String, List<TaxonomyLineage>> call(Row rowValue) throws Exception {
        String lineageId = rowValue.getString(rowValue.fieldIndex("LINEAGE_ID"));
        String lineageName = rowValue.getString(rowValue.fieldIndex("LINEAGE_NAME"));
        String lineageCommon = rowValue.getString(rowValue.fieldIndex("LINEAGE_COMMON"));
        String lineageRank = rowValue.getString(rowValue.fieldIndex("LINEAGE_RANK"));
        String lineageHidden = rowValue.getString(rowValue.fieldIndex("LINEAGE_HIDDEN"));

        String[] lineageIdArray = lineageId.substring(1).split("\\|");
        String[] lineageNameArray = lineageName.substring(1).split("\\|");
        String[] lineageRankArray = lineageRank.substring(1).split("\\|");
        String[] lineageCommonArray = lineageCommon.substring(1).split("\\|");
        String[] lineageHiddenArray = lineageHidden.substring(1).split("\\|");

        String taxId = lineageIdArray[0];
        List<TaxonomyLineage> lineageList = new ArrayList<>();
        for (int i = 1; i < lineageIdArray.length - 1; i++) {
            TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
            builder.taxonId(Long.parseLong(lineageIdArray[i]));
            builder.scientificName(lineageNameArray[i]);
            builder.commonName(lineageCommonArray[i].trim());
            builder.hidden(lineageHiddenArray[i].equals("1"));
            if (Utils.notNullOrEmpty(lineageRankArray[i])) {
                try {
                    builder.rank(TaxonomyRank.valueOf(lineageRankArray[i].toUpperCase()));
                } catch (IllegalArgumentException iae) {
                    builder.rank(TaxonomyRank.NO_RANK);
                }
            }
            lineageList.add(builder.build());
        }

        return new Tuple2<>(taxId, lineageList);
    }
}

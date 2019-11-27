package indexer.taxonomy;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.core.util.Utils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyLineageRowMapper implements PairFunction<Row, String, List<TaxonomyLineage>>, Serializable {

    private static final long serialVersionUID = -7723532417214033169L;

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

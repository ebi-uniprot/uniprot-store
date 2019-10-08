package indexer.uniref;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lgonzales
 * @since 2019-10-04
 */
public class DatasetUnirefRowConverter implements MapFunction<UniRefEntry, Row>, Serializable {

    private static final long serialVersionUID = 4552426388773275931L;

    @Override
    public Row call(UniRefEntry uniRefEntry) throws Exception {
        List<String> accessionIds = new ArrayList<>();
        List<String> upIds = new ArrayList<>();
        if (uniRefEntry.getRepresentativeMember().getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
            accessionIds.add(uniRefEntry.getRepresentativeMember().getMemberId());
        } else {
            upIds.add(uniRefEntry.getRepresentativeMember().getMemberId());
        }
        if (Utils.notEmpty(uniRefEntry.getMembers())) {
            uniRefEntry.getMembers().forEach(uniRefMember -> {
                if (uniRefMember.getMemberIdType() == UniRefMemberIdType.UNIPROTKB) {
                    accessionIds.add(uniRefMember.getMemberId());
                } else {
                    upIds.add(uniRefMember.getMemberId());
                }
            });
        }
        final Object[] rowColumns = {uniRefEntry.getId().getValue(), accessionIds.toArray(new String[0]), upIds.toArray(new String[0]), SerializationUtils.serialize(uniRefEntry)};
        return new GenericRowWithSchema(rowColumns, getRowSchema());
    }

    public static StructType getRowSchema() {
        StructType structType = new StructType();
        structType = structType.add("clusterId", DataTypes.StringType, false);
        structType = structType.add("accessionIds", DataTypes.createArrayType(DataTypes.StringType), true);
        structType = structType.add("upIds", DataTypes.createArrayType(DataTypes.StringType), true);
        structType = structType.add("unirefEntry", DataTypes.BinaryType, false);
        return structType;
    }
}

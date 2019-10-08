package indexer.uniprot;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.uniprot.core.uniprot.UniProtEntry;

import java.io.Serializable;

/**
 * @author lgonzales
 * @since 2019-10-04
 */
public class DatasetUniprotRowConverter implements MapFunction<UniProtEntry, Row>, Serializable {

    private static final long serialVersionUID = 4552426388773275931L;

    @Override
    public Row call(UniProtEntry uniProtEntry) throws Exception {
        //Kryo kryo = new Kryo();
        //kryo.setRegistrationRequired(false);
        //final Output out = new Output(1024);
        //kryo.writeObject(out, uniProtEntry);
        //final Object[] rowColumns = {uniProtEntry.getPrimaryAccession().getValue(),out.getBuffer()};
        final Object[] rowColumns = {uniProtEntry.getPrimaryAccession().getValue(), SerializationUtils.serialize(uniProtEntry)};
        return new GenericRowWithSchema(rowColumns, getRowSchema());
    }

    public static StructType getRowSchema() {
        StructType structType = new StructType();
        structType = structType.add("accession", DataTypes.StringType, false);
        structType = structType.add("uniprotEntry", DataTypes.BinaryType, false);
        return structType;
    }
}

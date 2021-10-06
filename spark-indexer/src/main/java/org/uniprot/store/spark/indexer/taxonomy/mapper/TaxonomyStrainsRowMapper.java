package org.uniprot.store.spark.indexer.taxonomy.mapper;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain.StrainNameClass;

import scala.Tuple2;

public class TaxonomyStrainsRowMapper implements PairFunction<Row, String, Strain>, Serializable {

    private static final long serialVersionUID = -7632620308030058658L;

    @Override
    public Tuple2<String, Strain> call(Row row) throws Exception {
        BigDecimal taxId = row.getDecimal(row.fieldIndex("TAX_ID"));
        BigDecimal strainId = row.getDecimal(row.fieldIndex("STRAIN_ID"));
        String strainName = row.getString(row.fieldIndex("NAME"));
        String nameClass = row.getString(row.fieldIndex("NAME_CLASS"));

        StrainNameClass strainClass = StrainNameClass.fromQuery(nameClass);
        Strain strain = new Strain(strainId.longValue(), strainClass, strainName);
        return new Tuple2<>(String.valueOf(taxId.longValue()), strain);
    }
}

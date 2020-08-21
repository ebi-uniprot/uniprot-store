package org.uniprot.store.spark.indexer.proteome.converter;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.xml.uniprot.XmlConverterHelper;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.ID;
import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.UPDATED;

/**
 * Converts XML {@link Row} instances to {@link ProteomeEntry} instances.
 * @author sahmad
 * @created 21/08/2020
 */
public class RowProteomeEntryConverter implements Function<Row, ProteomeEntry>, Serializable {

    private static final long serialVersionUID = -6073762696467389831L;


    @Override
    public ProteomeEntry call(Row row) throws Exception {// TODO need more data? like name and taxonomy
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
        builder.proteomeId(row.getString(row.fieldIndex("_upid")));
        String xmlUpdatedDate = row.getString(row.fieldIndex("_modified"));
        XMLGregorianCalendar xmlDate = DatatypeFactory.newInstance()
                .newXMLGregorianCalendar(xmlUpdatedDate);
        builder.modified(XmlConverterHelper.dateFromXml(xmlDate));
        builder.sourceDb(row.getString(row.fieldIndex("_source")));
        return builder.build();
    }
}

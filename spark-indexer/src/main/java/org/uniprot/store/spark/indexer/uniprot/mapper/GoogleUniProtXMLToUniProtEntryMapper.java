package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.StringReader;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.xml.jaxb.uniprot.Uniprot;
import org.uniprot.core.xml.uniprot.GoogleUniProtEntryConverter;

import lombok.extern.slf4j.Slf4j;
import scala.Serializable;
import scala.Tuple2;

@Slf4j
public class GoogleUniProtXMLToUniProtEntryMapper
        implements PairFunction<String, String, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = 6486460460998546116L;

    @Override
    public Tuple2<String, UniProtKBEntry> call(String entryXml) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance("org.uniprot.core.xml.jaxb.uniprot");
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

            String wrappedXml =
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                            + "<uniprot xmlns=\"http://uniprot.org/uniprot\">"
                            + entryXml
                            + "</uniprot>";

            StringReader reader = new StringReader(wrappedXml);
            Uniprot uniprot = (Uniprot) unmarshaller.unmarshal(reader);

            GoogleUniProtEntryConverter converter = new GoogleUniProtEntryConverter();
            List<UniProtKBEntry> entries =
                    uniprot.getEntry().stream().map(converter::fromXml).toList();
            UniProtKBEntry entry = entries.get(0);
            return new Tuple2<>(entry.getPrimaryAccession().getValue(), entry);
        } catch (JAXBException e) {
            log.error("Error while parsing google protnlm uniprot XML:{}", entryXml, e);
            throw new RuntimeException(e);
        }
    }
}

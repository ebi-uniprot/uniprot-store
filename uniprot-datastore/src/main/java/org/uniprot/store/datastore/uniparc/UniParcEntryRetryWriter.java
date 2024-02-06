package org.uniprot.store.datastore.uniparc;

import java.io.StringWriter;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;

/**
 * @author lgonzales
 * @since 2020-03-03
 */
@Slf4j
public class UniParcEntryRetryWriter extends ItemRetryWriter<UniParcEntry, UniParcEntry> {

    private static final String HEADER =
            "<uniparc xmlns=\"http://uniprot.org/uniparc\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://uniprot.org/uniparc http://www.uniprot.org/docs/uniparc.xsd\">";
    private static final String FOOTER = "</uniparc>";
    private final UniParcEntryConverter converter;
    private Marshaller marshaller;

    public UniParcEntryRetryWriter(Store<UniParcEntry> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
        this.converter = new UniParcEntryConverter();
        initMarshaller("org.uniprot.core.xml.jaxb.uniparc");
    }

    private void initMarshaller(String jaxbPackage) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(jaxbPackage);
            marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        } catch (Exception e) {
            log.error("UniParc JAXB initiallation failed", e);
        }
    }

    @Override
    protected String extractItemId(UniParcEntry entry) {
        return entry.getUniParcId().getValue();
    }

    @Override
    protected String entryToString(UniParcEntry entry) {
        if (this.marshaller == null) return "";
        StringWriter writer = new StringWriter();
        Entry xmlEntry = converter.toXml(entry);
        try {
            this.marshaller.marshal(xmlEntry, writer);
            writer.write("\n");
        } catch (Exception e) {
            log.error("UniParc writing xml entry failed");
        }
        return writer.toString();
    }

    @Override
    public UniParcEntry itemToEntry(UniParcEntry item) {
        return item;
    }

    @Override
    protected String getHeader() {
        return HEADER;
    }

    @Override
    protected String getFooter() {
        return FOOTER;
    }
}

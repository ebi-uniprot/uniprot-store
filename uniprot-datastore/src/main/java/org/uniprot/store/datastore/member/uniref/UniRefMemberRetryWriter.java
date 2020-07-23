package org.uniprot.store.datastore.member.uniref;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.uniref.RepresentativeMemberConverter;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRefMemberRetryWriter
        extends ItemRetryWriter<RepresentativeMember, RepresentativeMember> {
    private static final String HEADER =
            "<UniRef xmlns=\"http://uniprot.org/uniref\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                    + "xsi:schemaLocation=\"http://uniprot.org/uniref http://www.uniprot.org/docs/uniref.xsd\">";
    private static final String FOOTER = "</UniRef>";
    private final RepresentativeMemberConverter converter;
    private Marshaller marshaller;

    public UniRefMemberRetryWriter(
            Store<RepresentativeMember> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
        this.converter = new RepresentativeMemberConverter();
        initMarshaller("org.uniprot.core.xml.jaxb.uniref");
    }

    private void initMarshaller(String jaxbPackage) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(jaxbPackage);
            marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        } catch (Exception e) {
            log.error("JAXB initiallation failed", e);
        }
    }

    @Override
    protected String extractItemId(RepresentativeMember item) {
        return item.getMemberId();
    } // TODO

    @Override
    protected String entryToString(RepresentativeMember entry) {
        if (this.marshaller == null) return "";
        MemberType xmlEntry = converter.toXml(entry);
        StringWriter writer = new StringWriter();
        try {
            this.marshaller.marshal(xmlEntry, writer);
            writer.write("\n");
            return writer.toString();
        } catch (Exception e) {
            log.error("writing xml entry failed");
        }

        return "";
    }

    @Override
    public RepresentativeMember itemToEntry(RepresentativeMember item) {
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

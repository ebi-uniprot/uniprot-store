package org.uniprot.store.config.uniprotkb;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.uniprot.store.config.returnfield.model.ReturnField;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created 03/03/2020
 *
 * @author Edd
 */
public class CreateJsonForResultFields {
    private static List<InternalResultField> fields;
    private static ObjectMapper om;

    public static void main(String[] args) throws IOException {
        om = new ObjectMapper();
        fields = new ArrayList<>();
        JsonNode rootNode =
                om.readTree(
                        new File(
                                "/home/eddturner/working/intellij/website/uniprot-store/uniprot-config/src/main/resources/resultfields.json"));

        AtomicInteger itemPosition = new AtomicInteger();
        rootNode.iterator().forEachRemaining(node -> createFieldForGroup(node, itemPosition));

        printFields();
    }

    private static void createFieldForGroup(JsonNode node, AtomicInteger itemPosition) {
        String groupName = node.get("groupName").asText();
        int position = itemPosition.getAndIncrement();
        InternalResultField resultField = new InternalResultField();
        resultField.setGroupName(groupName);
        resultField.setItemType("group");
        resultField.setSeqNumber(position);
        resultField.setIsDatabaseGroup(node.get("isDatabase").asBoolean());
        resultField.setId(groupName.replace(" ", "_").toLowerCase());
        fields.add(resultField);

        AtomicInteger childPosition = new AtomicInteger();
        node.get("fields")
                .iterator()
                .forEachRemaining(child -> createFieldForChild(child, resultField, childPosition));
    }

    private static void createFieldForChild(
            JsonNode child, InternalResultField parent, AtomicInteger childPosition) {
        String label = child.get("label").asText();
        InternalResultField resultField = new InternalResultField();
        resultField.setName(child.get("name").asText());
        resultField.setLabel(label);
        resultField.setParentId(parent.getGroupName());
        resultField.setChildNumber(childPosition.getAndIncrement());
        resultField.setItemType("single");
        resultField.setId(parent.getId() + "/" + label.replace(" ", "_").toLowerCase());
        fields.add(resultField);
    }

    private static void printFields() {
        try {
            System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(fields));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class InternalResultField extends ReturnField {
        private Integer seqNumber;
        private String parentId;
        private Integer childNumber;
        private String itemType;
        private String groupName;
        private Boolean isDatabaseGroup;
        private String id;
    }
}

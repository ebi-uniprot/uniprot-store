package org.uniprot.store.config.uniprotkb;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

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

    private static List<ResultField> fields;
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
        ResultField resultField =
                ResultField.builder()
                        .groupName(groupName)
                        .itemType("group")
                        .seqNumber(position)
                        .isDatabaseGroup(node.get("isDatabase").asBoolean())
                        .id(groupName.replace(" ", "_").toLowerCase())
                        .build();
        fields.add(resultField);

        AtomicInteger childPosition = new AtomicInteger();
        node.get("fields")
                .iterator()
                .forEachRemaining(child -> createFieldForChild(child, resultField, childPosition));
    }

    private static void createFieldForChild(
            JsonNode child, ResultField parent, AtomicInteger childPosition) {
        String label = child.get("label").asText();
        ResultField resultField =
                ResultField.builder()
                        .name(child.get("name").asText())
                        .label(label)
                        .parentId(parent.getGroupName())
                        .childNumber(childPosition.getAndIncrement())
                        .itemType("single")
                        .id(parent.getId()+"/"+label.replace(" ", "_").toLowerCase())
                        .build();
        fields.add(resultField);
    }

    private static void printFields() {
        try {
            System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(fields));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Builder
    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class ResultField {
        private Integer seqNumber;
        private String parentId;
        private Integer childNumber;
        private String itemType;
        private String label;
        private String name;
        private String path;
        private String filter;
        private String groupName;
        private Boolean isDatabaseGroup;
        private String id;
    }
}

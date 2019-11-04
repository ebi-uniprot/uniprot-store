package org.uniprot.store.search.field;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.uniprot.store.search.domain.*;
import org.uniprot.store.search.domain.impl.*;

import java.io.File;
import java.io.InputStream;
import java.util.List;

/**
 * Created 04/11/19
 *
 * @author Edd
 */
public class JsonToYamlConverter {
    private static final String FILENAME = "uniprot/result_field.json";
    public static void main(String[] args) {
        ObjectMapper yamlMapper = getYamlMapper();
        ObjectMapper jsonMapper = JsonConfig.getJsonMapper();
        jsonMapper.setAnnotationIntrospector(new IgnoreInheritedIntrospector());
        jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        jsonMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try (InputStream is =
                JsonToYamlConverter.class.getClassLoader().getResourceAsStream(FILENAME)) {
            List<FieldGroupImpl> fields =
                    jsonMapper.readValue(is, new TypeReference<List<FieldGroupImpl>>() {});
//
//            PrintWriter p = new PrintWriter("my.yaml");
//            yamlMapper.writerWithDefaultPrettyPrinter().writeValue(p, fields);
//            p.close();

            List<FieldGroupImpl> readFields =
                    yamlMapper.readValue(
                            new File("my.yaml"), new TypeReference<List<FieldGroupImpl>>() {});

            if (fields.equals(readFields)) {
                System.out.println("yaml == json");
            } else {
                System.out.println("ERROR: yaml != json");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ObjectMapper getYamlMapper(){
        final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        SimpleModule mod = new SimpleModule();
        mod.addAbstractTypeMapping(EvidenceGroup.class, EvidenceGroupImpl.class);
        mod.addAbstractTypeMapping(EvidenceItem.class, EvidenceItemImpl.class);
        mod.addAbstractTypeMapping(FieldGroup.class, FieldGroupImpl.class);
        mod.addAbstractTypeMapping(Field.class,FieldImpl.class);
        mod.addAbstractTypeMapping(SearchItem.class, UniProtSearchItem.class);
        mod.addAbstractTypeMapping(Tuple.class, TupleImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }

    private static class IgnoreInheritedIntrospector extends JacksonAnnotationIntrospector {
        @Override
        public boolean hasIgnoreMarker(final AnnotatedMember m) {
            return m.getDeclaringClass() == Deserializers.Base.class || super.hasIgnoreMarker(m);
        }
    }
}

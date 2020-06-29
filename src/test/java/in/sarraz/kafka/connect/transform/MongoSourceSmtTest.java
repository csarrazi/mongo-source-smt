package in.sarraz.kafka.connect.transform;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoSourceSmtTest {
    private final MongoSourceSmt<SourceRecord> xform = new MongoSourceSmt<SourceRecord>();

    @AfterEach
    void tearDown() {
        xform.close();
    }

    @Test
    public void objectId() {
        xform.configure(Collections.<String, String>emptyMap());
        String oid = "5ef378f95891b15f8d437766";

        final String value = new Document("_id", new ObjectId(oid)).toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        final SourceRecord transformedRecord = xform.apply(record);

        final String expectedKey = "{\"_id\": {\"$oid\": \"" + oid + "\"}}";
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    public void array() {
        xform.configure(Collections.<String, Object>emptyMap());
        List<Integer> myArr = Arrays.asList(1, 2, 3, 4);

        final String value = new Document("_id", myArr).toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        assertThrows(ConnectException.class, () -> xform.apply(record));
    }

    @Test
    void document() {
        xform.configure(Collections.<String, String>emptyMap());

        final String value = new Document("_id", new Document("a", 1).append("b", "value")).toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        final SourceRecord transformedRecord = xform.apply(record);

        final String expectedKey = "{\"_id\": {\"a\": 1, \"b\": \"value\"}}";
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    void string() {
        xform.configure(Collections.<String, String>emptyMap());

        final String value = new Document("_id", "abcdef").toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        final SourceRecord transformedRecord = xform.apply(record);

        final String expectedKey = "{\"_id\": \"abcdef\"}";
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    void invalidPath() {
        assertThrows(ConnectException.class, () -> xform.configure(Collections.singletonMap(MongoSourceSmt.PATH_CONFIG, "_id.")));
    }

    @Test
    void missingPath() {
        xform.configure(Collections.singletonMap(MongoSourceSmt.PATH_CONFIG, "fullDocument"));

        final String value = new Document("_id", 123).toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        assertThrows(ConnectException.class, () -> xform.apply(record));
    }

    @Test
    void embeddedField() {
        xform.configure(Collections.singletonMap(MongoSourceSmt.PATH_CONFIG, "fullDocument.a"));

        final String value = new Document("fullDocument", new Document("a", 1).append("b", "value")).toJson();
        final SourceRecord record = new SourceRecord(null, null, "", Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, value);

        final SourceRecord transformedRecord = xform.apply(record);

        final String expectedKey = "{\"_id\": 1}";
        assertEquals(expectedKey, transformedRecord.key());
    }
}

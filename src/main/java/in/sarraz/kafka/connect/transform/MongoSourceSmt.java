package in.sarraz.kafka.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MongoSourceSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String PATH_CONFIG = "path";
    public static final String ID_FIELD = "_id";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATH_CONFIG, ConfigDef.Type.STRING, ID_FIELD, ConfigDef.Importance.MEDIUM,
                    "Path from which the key should be extracted");

    private String path;

    public void configure(final Map<String, ?> configs) {
        final MongoSourceSmtConfig config = new MongoSourceSmtConfig(CONFIG_DEF, configs);
        path = config.getString(PATH_CONFIG);
        if (path.endsWith(".")) {
            throw new ConnectException("Path should not end with a dot (\".\") character.");
        }
    }

    public R apply(final R r) {
        if (r.valueSchema().type() != Schema.Type.STRING) {
            throw new ConnectException("Only STRING schema types are supported for this SMT");
        }

        final Document doc = Document.parse(r.value().toString());

        final Object newKey = doc.getEmbedded(Arrays.asList(path.split("\\.")), Object.class);

        if (null == newKey) {
            throw new ConnectException("Document does not contain a \"" + path + "\" field or its value is null.");
        }

        if (newKey instanceof List || newKey.getClass().isArray()) {
            throw new ConnectException("The value found in path should neither be an array or a list");
        }

        String keyString = new Document(ID_FIELD, newKey).toJson();

        return r.newRecord(r.topic(), r.kafkaPartition(), Schema.STRING_SCHEMA, keyString, r.valueSchema(), r.value(), r.timestamp());
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {
    }
}

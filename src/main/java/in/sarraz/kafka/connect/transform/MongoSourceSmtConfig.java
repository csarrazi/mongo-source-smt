package in.sarraz.kafka.connect.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MongoSourceSmtConfig extends AbstractConfig {
    public MongoSourceSmtConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals, false);
    }
}

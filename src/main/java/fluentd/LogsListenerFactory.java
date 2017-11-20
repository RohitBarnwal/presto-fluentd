package logs;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class LogsListenerFactory implements EventListenerFactory {

    @Override
    public String getName() {
        return "presto-logs";
   }

    @Override
    public EventListener create(Map<String, String> config) {
        return new LogsListener(config);
   }
}

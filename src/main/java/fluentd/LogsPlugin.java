package logs;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableList;

public class LogsPlugin implements Plugin {

   @Override
   public Iterable<EventListenerFactory> getEventListenerFactories() {
      // EventListenerFactory listenerFactory = new LogsListenerFactory();
      return ImmutableList.of(new LogsListenerFactory());
   }
}
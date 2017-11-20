package logs;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
// import io.airlift.log.Logger;

import java.io.IOException;
import java.util.Map;
import java.io.FileWriter;
import java.util.HashMap;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;


public class LogsListener implements EventListener{
   FileWriter writer;

   public LogsListener(){
      try {
         writer = new FileWriter("/var/lib/presto/queries.log");

      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   public LogsListener(Map<String, String> config){
      try {
         writer = new FileWriter("/var/lib/presto/queries.log");

      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {

        Map<String, Object> event = new HashMap<>();

        // QueryMetadata
        event.put("queryId", queryCompletedEvent.getMetadata().getQueryId());
        event.put("transactionId", queryCompletedEvent.getMetadata().getTransactionId());

        event.put("query", queryCompletedEvent.getMetadata().getQuery());
        event.put("queryState", queryCompletedEvent.getMetadata().getQueryState());

        event.put("uri", queryCompletedEvent.getMetadata().getUri().toString());

        event.put("plan", queryCompletedEvent.getMetadata().getPlan());
        event.put("payload", queryCompletedEvent.getMetadata().getPayload());


        // QueryStatistics
        event.put("cpuTime", queryCompletedEvent.getStatistics().getCpuTime().toMillis());
        event.put("wallTime", queryCompletedEvent.getStatistics().getWallTime().toMillis());
        event.put("queuedTime", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
        if(queryCompletedEvent.getStatistics().getAnalysisTime().isPresent()) {
            event.put("analysisTime", queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis());
        }
        if(queryCompletedEvent.getStatistics().getDistributedPlanningTime().isPresent()) {
            event.put("distributedPlanningTime", queryCompletedEvent.getStatistics().getDistributedPlanningTime().get().toMillis());
        }
        event.put("peakMemoryBytes", queryCompletedEvent.getStatistics().getPeakMemoryBytes());
        event.put("totalBytes", queryCompletedEvent.getStatistics().getTotalBytes());
        event.put("totalRows", queryCompletedEvent.getStatistics().getTotalRows());
        event.put("cumulativeMemory", queryCompletedEvent.getStatistics().getCumulativeMemory());
        event.put("completedSplits", queryCompletedEvent.getStatistics().getCompletedSplits());

        // QueryContext
        event.put("user", queryCompletedEvent.getContext().getUser());
        if(queryCompletedEvent.getContext().getPrincipal().isPresent()) {
            event.put("principal", queryCompletedEvent.getContext().getPrincipal().get());
        }
        if(queryCompletedEvent.getContext().getRemoteClientAddress().isPresent()) {
            event.put("remoteClientAddress", queryCompletedEvent.getContext().getRemoteClientAddress().get());
        }
        if(queryCompletedEvent.getContext().getUserAgent().isPresent()) {
            event.put("userAgent", queryCompletedEvent.getContext().getUserAgent().get());
        }
        if(queryCompletedEvent.getContext().getClientInfo().isPresent()) {
            event.put("clientInfo", queryCompletedEvent.getContext().getClientInfo().get());
        }
        if(queryCompletedEvent.getContext().getSource().isPresent()) {
            event.put("source", queryCompletedEvent.getContext().getSource().get());
        }
        if(queryCompletedEvent.getContext().getCatalog().isPresent()) {
            event.put("catalog", queryCompletedEvent.getContext().getCatalog().get());
        }
        if(queryCompletedEvent.getContext().getSchema().isPresent()) {
            event.put("schema", queryCompletedEvent.getContext().getSchema().get());
        }
        if(queryCompletedEvent.getContext().getResourceGroupName().isPresent()) {
            event.put("resourceGroupName", queryCompletedEvent.getContext().getResourceGroupName().get());
        }

        // QueryFailureInfo
        if(queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();
            event.put("errorCode", queryFailureInfo.getErrorCode());
            event.put("failureHost", queryFailureInfo.getFailureHost().orElse(""));
            event.put("failureMessage", queryFailureInfo.getFailureMessage().orElse(""));
            event.put("failureTask", queryFailureInfo.getFailureTask().orElse(""));
            event.put("failureType", queryFailureInfo.getFailureType().orElse(""));
            event.put("failuresJson", queryFailureInfo.getFailuresJson());
        }

        event.put("createTime", queryCompletedEvent.getCreateTime().toEpochMilli());
        event.put("endTime", queryCompletedEvent.getEndTime().toEpochMilli());
        event.put("executionStartTime", queryCompletedEvent.getExecutionStartTime().toEpochMilli());



      try {
         writer = new FileWriter("/var/lib/presto/queries.log", true);
         // // writer.append(" Query : " + queryCompletedEvent.getMetadata().getQuery()+"\r\n ");

         // Class<?> clazz = queryCompletedEvent.getMetadata().getClass();

         // for(Field field : clazz.getDeclaredFields()) {
         //     //you can also use .toGenericString() instead of .getName(). This will
         //     //give you the type information as well.
         //    writer.append(field.getName()+"\r\n ");
         // }

         // writer.close();

         // for (Field field : queryCompletedEvent.class.getDeclaredFields()) {
         //     //get the static type of the field
         //     Class<?> fieldType = field.getType();
         //     //if it's String,
         //     if (fieldType == String.class) {
         //         // save/use field
         //     }
         //     //if it's String[],
         //     else if (fieldType == String[].class) {
         //         // save/use field
         //     }
         //     //if it's List or a subtype of List,
         //     else if (List.class.isAssignableFrom(fieldType)) {
         //         //get the type as generic
         //         ParameterizedType fieldGenericType =
         //                 (ParameterizedType)field.getGenericType();
         //         //get it's first type parameter
         //         Class<?> fieldTypeParameterType =
         //                 (Class<?>)fieldGenericType.getActualTypeArguments()[0];
         //         //if the type parameter is String,
         //         if (fieldTypeParameterType == String.class) {
         //             // save/use field
         //         }
         //     }
         // }
         // writer.close();













         try {
            writer = new FileWriter("/var/lib/presto/queries.log", true);
            event.forEach((key, value) -> {
               try { 
                     writer.append(key + "  " + value + System.lineSeparator()); 
               }
               catch (IOException ex) { throw new UncheckedIOException(ex); }
            });
            writer.append("\r\n ");
            writer.close();
         } catch(UncheckedIOException ex) { 
            throw ex.getCause(); 
         }


      } catch (IOException e) {
         e.printStackTrace();
      }








   }
}
/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.data.filesystem;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetReaderException;
import com.cloudera.cdk.data.spi.AbstractDatasetReader;
import com.cloudera.cdk.data.spi.ReaderWriterState;
import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.FaultTolerance;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Metrics;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MorphlineDatasetReader<E> extends AbstractDatasetReader<E> {

  private final FileSystem fs;
  private final Path path;
  private final Schema schema;
  private Class<E> recordClass;

  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasLookAhead = false;  
  private E next = null;  

  private final BlockingQueue<Record> queue;
  private CountDownLatch isClosing;
  private Thread thread;
  private final Throwable[] workerExceptionHolder = new Throwable[1];
  
  private final MorphlineContext morphlineContext;
  private final Command morphline;
  private final String morphlineFileAndId;
  
  private final Timer mappingTimer;
  private final Meter numRecords;
  private final Meter numFailedRecords;
  private final Meter numExceptionRecords;
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  public static final String MORPHLINE_QUEUE_CAPACITY = "morphlineQueueCapacity";
  
  /**
   * Morphline variables can be passed from flume.conf to the morphline, e.g.:
   * agent.sinks.solrSink.morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";
  
  private static final Record EOS = new Record(); // sentinel

  private static final Logger logger = LoggerFactory.getLogger(MorphlineDatasetReader.class);

  public MorphlineDatasetReader(FileSystem fileSystem, Path path, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(fileSystem != null, "FileSystem cannot be null");
    Preconditions.checkArgument(path != null, "Path cannot be null");
    Preconditions.checkArgument(descriptor != null, "DatasetDescriptor cannot be null");

    this.fs = fileSystem;
    this.path = path;
    this.schema = descriptor.getSchema();
    Preconditions.checkArgument(schema != null, "Schema cannot be null");
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for morphline files must be records of primitive types");
    
    this.state = ReaderWriterState.NEW;
    this.queue = new ArrayBlockingQueue(getInteger(descriptor, MORPHLINE_QUEUE_CAPACITY, 1000));
    
    try {
      this.recordClass = (Class<E>) Class.forName(schema.getFullName());
    } catch (ClassNotFoundException ignored) {
      logger.debug("Using generic record, cannot find: {}",
            schema.getFullName());
      this.recordClass = null;
    }

    String morphlineFile = descriptor.getProperty(MORPHLINE_FILE_PARAM);
    String morphlineId = descriptor.getProperty(MORPHLINE_ID_PARAM);
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    this.morphlineFileAndId = morphlineFile + "@" + morphlineId;
    
    FaultTolerance faultTolerance = new FaultTolerance(
        getBoolean(descriptor, FaultTolerance.IS_PRODUCTION_MODE, false), 
        getBoolean(descriptor, FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
        descriptor.getProperty(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES));
    
    this.morphlineContext = new MorphlineContext.Builder()
      .setExceptionHandler(faultTolerance)
      .setMetricRegistry(SharedMetricRegistries.getOrCreate(morphlineFileAndId))
      .build();
    
    Map morphlineVariables = new HashMap();
    for (String name : descriptor.listProperties()) {
        String variablePrefix = MORPHLINE_VARIABLE_PARAM + ".";
        if (name.startsWith(variablePrefix)) {
            morphlineVariables.put(name.substring(variablePrefix.length()), descriptor.getProperty(name));
        }
    }
    Config override = ConfigFactory.parseMap(morphlineVariables);

    this.morphline = new Compiler().compile(new File(morphlineFile), 
        morphlineId, morphlineContext, new Collector(), override);      
    
    this.mappingTimer = morphlineContext.getMetricRegistry().timer(
        MetricRegistry.name("morphline.app", Metrics.ELAPSED_TIME));
    this.numRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name("morphline.app", Metrics.NUM_RECORDS));
    this.numFailedRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name("morphline.app", Metrics.NUM_FAILED_RECORDS));
    this.numExceptionRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name("morphline.app", Metrics.NUM_EXCEPTION_RECORDS));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    hasLookAhead = false;  
    next = null;
    workerExceptionHolder[0] = null;
    isClosing = new CountDownLatch(1);
    queue.clear();

    final InputStream inputStream;
    try {
      inputStream = fs.open(path);
    } catch (IOException ex) {
      throw new DatasetReaderException("Cannot open path: " + path, ex);
    }

    
    thread = new Thread(new Runnable() {

      @Override
      public void run() { 
        try {
          doRun();
        } catch (RuntimeException e) {
          synchronized (workerExceptionHolder) {
            workerExceptionHolder[0] = e; // forward exception to consumer thread
          }
          logger.warn("Morphline thread exception", e);
        }
      }
      
      private void doRun() {
        try {
          Timer.Context timerContext = mappingTimer.time();
          try {
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, inputStream);
            try {
              Notifications.notifyBeginTransaction(morphline);
              Notifications.notifyStartSession(morphline);
              if (!morphline.process(record)) {
                numFailedRecords.mark();
                logger.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
              }
              Notifications.notifyCommitTransaction(morphline);
            } catch (RuntimeException t) {
              numExceptionRecords.mark();
              morphlineContext.getExceptionHandler().handleException(t, record);
              Notifications.notifyRollbackTransaction(morphline);
            } catch (CloseSignal e) {
              ; // nothing to do
            } 
          } finally {
            timerContext.stop();
            Closeables.closeQuietly(inputStream);
            Notifications.notifyShutdown(morphline);            
          }
        } finally {
          while (isClosing.getCount() > 0) {
            // Signal that we've reached EOS
            try {
              if (queue.offer(EOS, 100, TimeUnit.MILLISECONDS)) {
                break;
              }
            } catch (InterruptedException e) {
              ;
            }
          }      
        }
      }
      
    });
    
    thread.start();
    state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    
    if (!hasLookAhead) {
      next = advance();
      hasLookAhead = true;
    }
    return next != null;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    hasLookAhead = false;
    return next;
  }

  private E advance() {
    Record morphlineRecord;
    try {
      morphlineRecord = queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    
    assert morphlineRecord != null;
    if (morphlineRecord == EOS) {
      Throwable t; 
      synchronized (workerExceptionHolder) {
        t = workerExceptionHolder[0];
      }
      if (t != null) {
        throw new DatasetReaderException(t);
      }
      return null;
    }
    
    numRecords.mark();
    return makeRecord(morphlineRecord);
  }

  @Override
  public void close() {
    if (isClosing != null) {
      isClosing.countDown();    
    }
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }
    logger.debug("Closing reader on path:{}", path);
    state = ReaderWriterState.CLOSED;
    try {
      if (thread != null) {
        thread.join();
      }
    } catch (InterruptedException e) {
      ;
    } finally {
      queue.clear();
    }
  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }

  private E makeRecord(Record morphlineRecord) {
    if (recordClass != null) {
      E record = makeReflectRecord(morphlineRecord);
      if (record != null) {
        return record;
      }
    }
    return makeGenericRecord(morphlineRecord);
  }

  @SuppressWarnings("unchecked")
  private E makeGenericRecord(Record morphlineRecord) {
    GenericRecord record = new GenericData.Record(schema);
    fillIndexed(record, morphlineRecord);
    return (E) record;
  }

  @SuppressWarnings("unchecked")
  private E makeReflectRecord(Record morphlineRecord) {
    E record = (E) ReflectData.newInstance(recordClass, schema);
    if (record instanceof IndexedRecord) {
      fillIndexed((IndexedRecord) record, morphlineRecord);
    } else {
      fillReflect(record, morphlineRecord, schema);
    }
    return record;
  }

  private void fillIndexed(IndexedRecord record, Record morphlineRecord) {
    Schema schema = record.getSchema();
    for (Schema.Field field : schema.getFields()) {
      // FIXME support arrays, nested records, etc.
      String first = (String) morphlineRecord.getFirstValue(field.name());
      Object value = makeValue(first, field); 
      record.put(field.pos(), value);
    }
  }

  private static void fillReflect(Object record, Record morphlineRecord, Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      // FIXME support arrays, nested records, etc.
      String first = (String) morphlineRecord.getFirstValue(field.name());
      Object value = makeValue(first, field); 
      try {
        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor(
                field.name(), record.getClass(), null, setter(field.name()));
        propertyDescriptor.getWriteMethod().invoke(record, value);
      } catch (IntrospectionException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      } catch (InvocationTargetException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      } catch (IllegalAccessException ex) {
        throw new IllegalStateException("Cannot set property " + field.name() +
            " on " + record.getClass().getName(), ex);
      }
    }
  }

  private static String setter(String name) {
    return "set" +
        name.substring(0, 1).toUpperCase(Locale.ENGLISH) +
        name.substring(1);
  }

  private static Object makeValue(String string, Schema.Field field) {
    Object value = makeValue(string, field.schema());
    if (value != null || nullOk(field.schema())) {
      return value;
    } else {
      // this will fail if there is no default value
      return ReflectData.get().getDefaultValue(field);
    }
  }

  /**
   * Returns a the value as the first matching schema type or null.
   *
   * Note that if the value may be null even if the schema does not allow the
   * value to be null.
   *
   * @param string a String representation of the value
   * @param schema a Schema
   * @return the string coerced to the correct type from the schema or null
   */
  private static Object makeValue(String string, Schema schema) {
    if (string == null) {
      return null;
    }

    try {
      switch (schema.getType()) {
        case BOOLEAN:
          return Boolean.valueOf(string);
        case STRING:
          return string;
        case FLOAT:
          return Float.valueOf(string);
        case DOUBLE:
          return Double.valueOf(string);
        case INT:
          return Integer.valueOf(string);
        case LONG:
          return Long.valueOf(string);
        case ENUM:
          // TODO: translate to enum class
          if (schema.hasEnumSymbol(string)) {
            return string;
          } else {
            try {
              return schema.getEnumSymbols().get(Integer.valueOf(string));
            } catch (IndexOutOfBoundsException ex) {
              return null;
            }
          }
        case UNION:
          Object value = null;
          for (Schema possible : schema.getTypes()) {
            value = makeValue(string, possible);
            if (value != null) {
              return value;
            }
          }
          return null;
        default:
          // FIXED, BYTES, MAP, ARRAY, RECORD are not supported
          throw new DatasetReaderException(
              "Unsupported field type:" + schema.getType());
      }
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  /**
   * Returns whether null is allowed by the schema.
   *
   * @param schema a Schema
   * @return true if schema allows the value to be null
   */
  private static boolean nullOk(Schema schema) {
    if (Schema.Type.NULL == schema.getType()) {
      return true;
    } else if (Schema.Type.UNION == schema.getType()) {
      for (Schema possible : schema.getTypes()) {
        if (nullOk(possible)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private boolean getBoolean(DatasetDescriptor descriptor, String key, boolean defaultValue) {
    String value = descriptor.getProperty(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }

  private int getInteger(DatasetDescriptor descriptor, String key, int defaultValue) {
    String value = descriptor.getProperty(key);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return defaultValue;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private final class Collector implements Command {
    
    @Override
    public Command getParent() {
      return null;
    }
    
    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
      Preconditions.checkNotNull(record);
      while (true) {
        if (isClosing.getCount() <= 0) {
          throw new CloseSignal();
        }
        try {
          if (queue.offer(record, 100, TimeUnit.MILLISECONDS)) {
            return true;
          }
        } catch (InterruptedException e) {
          throw new CloseSignal();
        }
      }      
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class CloseSignal extends Error {    
  }
}
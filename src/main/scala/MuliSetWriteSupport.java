/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.spark_project.guava.collect.Multiset;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

public class MuliSetWriteSupport extends WriteSupport<Multiset<Object>> {
  MessageType schema;
  RecordConsumer recordConsumer;
  List<ColumnDescriptor> cols;

  // TODO: support specifying encodings and compression
  public MuliSetWriteSupport(MessageType schema) {
    this.schema = schema;
    this.cols = schema.getColumns();
  }

  @Override
  public WriteContext init(Configuration config) {
    return new WriteContext(schema, new HashMap<String, String>());
  }

  @Override
  public void prepareForWrite(RecordConsumer r) {
    recordConsumer = r;
  }

  @Override
  public void write(Multiset<Object> values) {
    for(Object value : values.elementSet()){
      recordConsumer.startMessage();
      recordConsumer.addBinary(stringToBinary(value.toString()));
      recordConsumer.addLong(values.count(value));
      recordConsumer.endMessage();
    }
  }

  private Binary stringToBinary(Object value) {
    return Binary.fromString(value.toString());
  }
}
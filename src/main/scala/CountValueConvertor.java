/**
 * Copyright 2013 ARRIS, Inc.
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

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;


public class CountValueConvertor extends GroupConverter {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final CharsetDecoder UTF8_DECODER = UTF8.newDecoder();

  private final Converter converters[];
  private final CountValueConvertor parent;

  public CountValueConvertor(GroupType schema) {
    this(schema, null);
  }

  public CountValueConvertor(GroupType schema, CountValueConvertor parent) {
    this.converters = new Converter[schema.getFieldCount()];
    this.parent = parent;

    int i = 0;
    for (Type field: schema.getFields()) {
      converters[i++] = createConverter(field);
    }
  }

  private Converter createConverter(Type field) {
    if (field.isPrimitive()) {
      OriginalType otype = field.getOriginalType();
      if (otype != null) {
        switch (otype) {
          case MAP: break;
          case LIST: break;
          case UTF8: return new StringConverter(field.getName());
          case MAP_KEY_VALUE: break;
          case ENUM: break;
        }
      }
      return new CountSetConverter(field.getName());
    }

    throw new UnknownError();
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {

  }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro;

import org.apache.hudi.internal.schema.Types;

public final class GeometryLType extends LogicalType {
  private static final GeometryLType INSTANCE = new GeometryLType();

  private static boolean registered = false;

  GeometryLType() {
    this(Types.GeometryType.get().toString());
  }

  GeometryLType(String logicalTypeName) {
    super(logicalTypeName);
  }

  public static Schema getSchema() {
    return INSTANCE.addToSchema(Schema.create(Schema.Type.BYTES));
  }

  public static synchronized void register() {
    if (!registered) {
      LogicalTypes.register(INSTANCE.getName(), new GeometryLogicalTypeFactory());
      registered = true;
    }
  }

  static class GeometryLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
      return GeometryLType.INSTANCE;
    }

    @Override
    public String getTypeName() {
      return GeometryLType.INSTANCE.getName();
    }
  }

}

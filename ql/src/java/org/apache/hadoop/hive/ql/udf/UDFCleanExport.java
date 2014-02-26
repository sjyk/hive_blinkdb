/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;

import java.util.Formatter;
import java.util.Locale;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

/**
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "clean_export",
    value = "_FUNC_(String file, Obj... args) - "
    + "function that can export a blinkdb sample to a csv file")
public class UDFCleanExport extends GenericUDTF {
  private ObjectInspector[] argumentOIs;
  private int recordPrimaryKey = 1;
  private double partitionId = 0.0;
  private FileWriter writer = null;
  private  ArrayList<String> fieldNames;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function CLEAN_EXPORT needs at least one argument.");
    }
      fieldNames = new ArrayList<String>();
      ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

      fieldNames.add("hash");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

      fieldNames.add("dup");
      fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

      for (int i = 0; i < arguments.length; i++) {
          fieldNames.add("col"+(i));
          fieldOIs.add(arguments[i]);
      }

      partitionId = Math.random();

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
            fieldOIs);
  }

    @Override
    public void close() throws HiveException {
    }

    public String getEncodedSha1Sum( String key ) {
        try {
            MessageDigest md = MessageDigest.getInstance( "SHA1" );
            md.update( key.getBytes() );
            return new BigInteger( 1, md.digest() ).toString(16);
        }
        catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

  @Override
  public void process(Object[] arguments) throws HiveException {
    ArrayList argumentList = new ArrayList();

    argumentList.add(new Text("1"));

    for (int i = 0; i < arguments.length; i++) {
      argumentList.add(arguments[i]);
    }

      argumentList.add(0,new Text(getEncodedSha1Sum(argumentList.toString())));

    recordPrimaryKey++; //increment the record key
    forward(argumentList.toArray());
  }

  @Override
  public String toString() {
    return "clean_export";
  }
}

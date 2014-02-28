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
package org.apache.hadoop.hive.ql.udf.approx_clean;

import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * GenericUDAFSum.
 *
 */
@Description(name = "approx_sum", value = "_FUNC_(x) - Returns the approximate sum of a set of numbers with error bars")
public class ApproxUDAFCountClean extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(ApproxUDAFCountClean.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 4) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }

        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:

                return new ApproxUDAFCountCleanLong();
            case FLOAT:
            case STRING:
            case DOUBLE:
                return new ApproxUDAFCountCleanDouble();
            case DATE:
            case TIMESTAMP:
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    /**
     * ApproxUDAFCountCleanDouble.
     *
     */
    public static class ApproxUDAFCountCleanDouble extends GenericUDAFEvaluator {
        // private PrimitiveObjectInspector inputOI;
        // private DoubleWritable result;

        // For PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector dupOI;
        private PrimitiveObjectInspector totalRowsOI;
        private PrimitiveObjectInspector sampleRowsOI;

        // For PARTIAL2 and FINAL
        private StructObjectInspector soi;
        private StructField countField;
        private StructField sumField;
        private StructField varianceField;
        private StructField totalRowsField;
        private StructField sampleRowsField;
        private LongObjectInspector countFieldOI;
        private DoubleObjectInspector sumFieldOI;
        private DoubleObjectInspector varianceFieldOI;
        private LongObjectInspector totalRowsFieldOI;
        private LongObjectInspector sampleRowsFieldOI;

        // For PARTIAL1 and PARTIAL2
        private Object[] partialResult;

        // For FINAL and COMPLETE
        Text result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 4);
            super.init(m, parameters);

            if (parameters.length == 4) {
                totalRowsOI = (PrimitiveObjectInspector) parameters[3];
                sampleRowsOI = (PrimitiveObjectInspector) parameters[2];
            }

            // result = new DoubleWritable(0);
            // inputOI = (PrimitiveObjectInspector) parameters[0];
            // return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                dupOI = (PrimitiveObjectInspector) parameters[1];
            } else {
                soi = (StructObjectInspector) parameters[0];

                countField = soi.getStructFieldRef("count");
                sumField = soi.getStructFieldRef("sum");
                varianceField = soi.getStructFieldRef("variance");
                totalRowsField = soi.getStructFieldRef("totalRows");
                sampleRowsField = soi.getStructFieldRef("sampleRows");

                countFieldOI = (LongObjectInspector) countField
                        .getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
                varianceFieldOI = (DoubleObjectInspector) varianceField
                        .getFieldObjectInspector();
                totalRowsFieldOI = (LongObjectInspector) totalRowsField
                        .getFieldObjectInspector();
                sampleRowsFieldOI = (LongObjectInspector) sampleRowsField
                        .getFieldObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                // The output of a partial aggregation is a struct containing
                // a long count and doubles sum and variance.

                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

                foi.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("empty");
                fname.add("count");
                fname.add("sum");
                fname.add("variance");
                fname.add("totalRows");
                fname.add("sampleRows");

                partialResult = new Object[6];
                partialResult[0] = new BooleanWritable();
                partialResult[1] = new LongWritable(0);
                partialResult[2] = new DoubleWritable(0);
                partialResult[3] = new DoubleWritable(0);
                partialResult[4] = new LongWritable(0);
                partialResult[5] = new LongWritable(0);

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
                        foi);

            } else {
                result = new Text();
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }

        }

        /** class for storing double sum value. */
        static class SumDoubleAgg implements AggregationBuffer {
            boolean empty;
            long count; // number of elements
            double sum;
            double variance; // sum[x-avg^2] (this is actually n times the variance)
            long totalRows;
            long sampleRows;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumDoubleAgg result = new SumDoubleAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg myagg = (SumDoubleAgg) agg;
            myagg.empty = true;
            myagg.count = 0;
            myagg.sum = 0;
            myagg.variance = 0;
            myagg.totalRows = 0;
            myagg.sampleRows = 0;
        }

        boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 4);

            if (parameters.length == 4) {
                ((SumDoubleAgg) agg).totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[3],
                        totalRowsOI);
                ((SumDoubleAgg) agg).sampleRows = PrimitiveObjectInspectorUtils.getLong(parameters[2],
                        sampleRowsOI);
            }

            Object p = parameters[0];
            Object d = parameters[1];
            if (p != null) {
                SumDoubleAgg myagg = (SumDoubleAgg) agg;
                try {
                    double v = 1.0;
                    double dv = PrimitiveObjectInspectorUtils.getDouble(d, dupOI);
                    myagg.count++;
                    myagg.sum += v/dv;
                    if (myagg.count > 1) {
                        double t = myagg.count * v/dv - myagg.sum;
                        myagg.variance += (t * t) / ((double) myagg.count * (myagg.count - 1));
                    }
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        LOG.warn(getClass().getSimpleName() + " "
                                + StringUtils.stringifyException(e));
                        LOG
                                .warn(getClass().getSimpleName()
                                        + " ignoring similar exceptions.");
                    }
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            // return terminate(agg);
            SumDoubleAgg myagg = (SumDoubleAgg) agg;
            ((BooleanWritable) partialResult[0]).set(myagg.empty);
            ((LongWritable) partialResult[1]).set(myagg.count);
            ((DoubleWritable) partialResult[2]).set(myagg.sum);
            ((DoubleWritable) partialResult[3]).set(myagg.variance);
            ((LongWritable) partialResult[4]).set(myagg.totalRows);
            ((LongWritable) partialResult[5]).set(myagg.sampleRows);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            if (partial != null) {

                SumDoubleAgg myagg = (SumDoubleAgg) agg;
                myagg.empty = false;

                Object partialCount = soi.getStructFieldData(partial, countField);
                Object partialSum = soi.getStructFieldData(partial, sumField);
                Object partialVariance = soi.getStructFieldData(partial, varianceField);
                Object partialTotalRows = soi.getStructFieldData(partial, totalRowsField);
                Object partialSampleRows = soi.getStructFieldData(partial, sampleRowsField);


                long n = myagg.count;
                long m = countFieldOI.get(partialCount);
                long q = totalRowsFieldOI.get(partialTotalRows);
                long r = sampleRowsFieldOI.get(partialSampleRows);
                myagg.totalRows = q;
                myagg.sampleRows = r;


                if (n == 0) {
                    // Just copy the information since there is nothing so far
                    myagg.variance = varianceFieldOI.get(partialVariance);
                    myagg.count = countFieldOI.get(partialCount);
                    myagg.sum = sumFieldOI.get(partialSum);
                }

                if (m != 0 && n != 0) {
                    // Merge the two partials

                    double a = myagg.sum;
                    double b = sumFieldOI.get(partialSum);

                    myagg.empty = false;
                    myagg.count += m;
                    myagg.sum += b;
                    double t = (m / (double) n) * a - b;
                    myagg.variance += varianceFieldOI.get(partialVariance)
                            + ((n / (double) m) / ((double) n + m)) * t * t;

                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumDoubleAgg myagg = (SumDoubleAgg) agg;
            if (myagg.empty) {
                return null;
            }

            double approx_sum = ((double) myagg.sum * myagg.totalRows) / myagg.sampleRows;
            double probability = ((double) myagg.count) / ((double) myagg.sampleRows);
            double mean = myagg.sum / myagg.count;

            LOG.info("Sum: " + approx_sum);
            LOG.info("TotalRows: " + myagg.totalRows);
            LOG.info("Probability: " + probability);
            LOG.info("Sampling Ratio: " + ((double) myagg.sampleRows) / myagg.totalRows);

            StringBuilder sb = new StringBuilder();
            sb.append(approx_sum);
            sb.append(" +/- ");
            sb.append(Math.ceil((1.96 * (1.0 * myagg.totalRows / myagg.sampleRows) * Math
                    .sqrt(probability
                            * (((1 - probability) * myagg.totalRows))))));
            sb.append(" (95% Confidence Clean) ");

            result.set(sb.toString());
            return result;

        }

    }

    /**
     * GenericUDAFSumLong.
     *
     */
    public static class ApproxUDAFCountCleanLong extends GenericUDAFEvaluator {
        // private PrimitiveObjectInspector inputOI;
        // private LongWritable result;

        // For PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector dupOI;
        private PrimitiveObjectInspector totalRowsOI;
        private PrimitiveObjectInspector sampleRowsOI;

        // For PARTIAL2 and FINAL
        private StructObjectInspector soi;
        private StructField countField;
        private StructField sumField;
        private StructField varianceField;
        private StructField totalRowsField;
        private StructField sampleRowsField;
        private LongObjectInspector countFieldOI;
        private LongObjectInspector sumFieldOI;
        private DoubleObjectInspector varianceFieldOI;
        private LongObjectInspector totalRowsFieldOI;
        private LongObjectInspector sampleRowsFieldOI;

        // For PARTIAL1 and PARTIAL2
        private Object[] partialResult;

        // For FINAL and COMPLETE
        Text result;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 4);
            super.init(m, parameters);

            if (parameters.length == 4) {
                totalRowsOI = (PrimitiveObjectInspector) parameters[3];
                sampleRowsOI = (PrimitiveObjectInspector) parameters[2];
            }

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                dupOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                soi = (StructObjectInspector) parameters[0];

                countField = soi.getStructFieldRef("count");
                sumField = soi.getStructFieldRef("sum");
                varianceField = soi.getStructFieldRef("variance");
                totalRowsField = soi.getStructFieldRef("totalRows");
                sampleRowsField = soi.getStructFieldRef("sampleRows");

                countFieldOI = (LongObjectInspector) countField
                        .getFieldObjectInspector();
                sumFieldOI = (LongObjectInspector) sumField.getFieldObjectInspector();
                varianceFieldOI = (DoubleObjectInspector) varianceField
                        .getFieldObjectInspector();
                totalRowsFieldOI = (LongObjectInspector) totalRowsField
                        .getFieldObjectInspector();
                sampleRowsFieldOI = (LongObjectInspector) sampleRowsField
                        .getFieldObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                // The output of a partial aggregation is a struct containing
                // a long count and doubles sum and variance.

                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

                foi.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("empty");
                fname.add("count");
                fname.add("sum");
                fname.add("variance");
                fname.add("totalRows");
                fname.add("sampleRows");

                partialResult = new Object[6];
                partialResult[0] = new BooleanWritable();
                partialResult[1] = new LongWritable(0);
                partialResult[2] = new LongWritable(0);
                partialResult[3] = new DoubleWritable(0);
                partialResult[4] = new LongWritable(0);
                partialResult[5] = new LongWritable(0);

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
                        foi);

            } else {
                result = new Text();
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }

        }

        /** class for storing double sum value. */
        static class CountLongAgg implements AggregationBuffer {
            boolean empty;
            long count; // number of elements
            long sum;
            double variance; // sum[x-avg^2] (this is actually n times the variance)
            long totalRows;
            long sampleRows;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CountLongAgg result = new CountLongAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            CountLongAgg myagg = (CountLongAgg) agg;
            myagg.empty = true;
            myagg.count = 0;
            myagg.sum = 0;
            myagg.variance = 0;
            myagg.totalRows = 0;
            myagg.sampleRows = 0;
        }

        private boolean warned = false;

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 4);

            Object p = parameters[0];
            Object d = parameters[1];
            if (p != null) {
                CountLongAgg myagg = (CountLongAgg) agg;

                if (parameters.length == 4) {
                    myagg.totalRows = PrimitiveObjectInspectorUtils.getLong(parameters[3],
                            totalRowsOI);
                    myagg.sampleRows = PrimitiveObjectInspectorUtils.getLong(parameters[2],
                            sampleRowsOI);
                }

                try {
                    long v = 1;
                    double dv = PrimitiveObjectInspectorUtils.getDouble(d, dupOI);
                    myagg.count++;
                    myagg.sum += v/dv;
                    if (myagg.count > 1) {
                        double t = myagg.count * v/dv - myagg.sum;
                        myagg.variance += (t * t) / ((double) myagg.count * (myagg.count - 1));
                    }
                } catch (NumberFormatException e) {
                    if (!warned) {
                        warned = true;
                        LOG.warn(getClass().getSimpleName() + " "
                                + StringUtils.stringifyException(e));
                    }
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {

            CountLongAgg myagg = (CountLongAgg) agg;
            ((BooleanWritable) partialResult[0]).set(myagg.empty);
            ((LongWritable) partialResult[1]).set(myagg.count);
            ((LongWritable) partialResult[2]).set(myagg.sum);
            ((DoubleWritable) partialResult[3]).set(myagg.variance);
            ((LongWritable) partialResult[4]).set(myagg.totalRows);
            ((LongWritable) partialResult[5]).set(myagg.sampleRows);

            return partialResult;

        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            if (partial != null) {
                CountLongAgg myagg = (CountLongAgg) agg;
                myagg.empty = false;

                Object partialCount = soi.getStructFieldData(partial, countField);
                Object partialSum = soi.getStructFieldData(partial, sumField);
                Object partialVariance = soi.getStructFieldData(partial, varianceField);
                Object partialTotalRows = soi.getStructFieldData(partial, totalRowsField);
                Object partialSampleRows = soi.getStructFieldData(partial, sampleRowsField);

                long n = myagg.count;
                long m = countFieldOI.get(partialCount);
                long q = totalRowsFieldOI.get(partialTotalRows);
                long r = sampleRowsFieldOI.get(partialSampleRows);

                myagg.totalRows = q;
                myagg.sampleRows = r;

                if (n == 0) {
                    // Just copy the information since there is nothing so far
                    myagg.variance = varianceFieldOI.get(partialVariance);
                    myagg.count = countFieldOI.get(partialCount);
                    myagg.sum = sumFieldOI.get(partialSum);
                }

                if (m != 0 && n != 0) {
                    // Merge the two partials

                    long a = myagg.sum;
                    long b = sumFieldOI.get(partialSum);

                    myagg.empty = false;
                    myagg.count += m;
                    myagg.sum += b;
                    double t = (m / (double) n) * a - b;
                    myagg.variance += varianceFieldOI.get(partialVariance)
                            + ((n / (double) m) / ((double) n + m)) * t * t;
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {

            CountLongAgg myagg = (CountLongAgg) agg;

            if (myagg.empty) {
                return null;
            }

            double approx_sum = ((double) myagg.sum * myagg.totalRows) / myagg.sampleRows;
            double probability = ((double) myagg.count) / ((double) myagg.sampleRows);
            double mean = ((double) myagg.sum) / myagg.count;

            LOG.info("Sum: " + approx_sum);
            LOG.info("TotalRows: " + myagg.totalRows);
            LOG.info("Probability: " + probability);
            LOG.info("Sampling Ratio: " + ((double) myagg.sampleRows) / myagg.totalRows);

            StringBuilder sb = new StringBuilder();
            sb.append(approx_sum);
            sb.append(" +/- ");
            sb.append(Math.ceil((1.96 * (1.0 * myagg.totalRows / myagg.sampleRows) * mean * Math
                    .sqrt(probability
                            * (((1 - probability) * myagg.totalRows))))));
            sb.append(" (95% Confidence Clean) ");

            result.set(sb.toString());
            return result;

        }

    }

}

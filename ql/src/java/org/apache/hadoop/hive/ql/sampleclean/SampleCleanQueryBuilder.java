package org.apache.hadoop.hive.ql.sampleclean;

import java.util.ArrayList;
import java.util.Collection;

public class SampleCleanQueryBuilder {

	private SampleCleanHiveTableOps tableOperator = null;

	public SampleCleanQueryBuilder()
	{
		tableOperator = new SampleCleanHiveTableOps();
	}

	public String createSample(String sampleName, String baseTable, ArrayList<String> schema,  double samplingProb)
	{
		ArrayList<String> sampleCleanExtraSchema = new ArrayList<String>();
		sampleCleanExtraSchema.add("hash");
		sampleCleanExtraSchema.add("dup");

		return tableOperator.createTableAs(sampleName+"_dirty",tableOperator.materializeUDTFView("clean_export", baseTable, schema, sampleCleanExtraSchema)) + tableOperator.withSampling(samplingProb);
	}

	public String createEmptyCleanTable(String sampleName, String baseTable, ArrayList<String> schema)
	{
		ArrayList<String> sampleCleanExtraSchema = new ArrayList<String>();
		sampleCleanExtraSchema.add("hash");
		sampleCleanExtraSchema.add("dup");
		sampleCleanExtraSchema.addAll(schema);

		ArrayList<String> types = new ArrayList<String>();
		types.add("string");
		types.add("int");

		for(int i=0;i<schema.size();i++)
			types.add("string");

		return tableOperator.createTypedTableWithCSV(sampleName+"_clean", sampleCleanExtraSchema, types);
	}

	public String copyDirtyToClean(String sampleName)
	{
		return tableOperator.copyTableTo(sampleName+"_dirty",sampleName+"_clean");
	}

	public String rawSCQuery(String sampleName, String aggFunc, String attribute, String predicate, String groupBy, ArrayList<String> schema, long sampleSize, long datasetSize)
	{
		if (aggFunc.equalsIgnoreCase("COUNT") || aggFunc.equalsIgnoreCase("SUM") )
			return "SELECT approx_" + aggFunc + "_clean("+attribute+ " , dup ,"+sampleSize +" , "+datasetSize+" ) FROM " + sampleName + "_clean "+ tableOperator.wherePredicate(predicate) + tableOperator.groupBy(groupBy);
		else
			return "SELECT approx_" + aggFunc + "_clean("+attribute+" , dup) FROM " + sampleName + "_clean "+ tableOperator.wherePredicate(predicate) + tableOperator.groupBy(groupBy);
	}

	public String normalizedSCQuery(String sampleName, String aggFunc, String attribute, String predicate, String groupBy, ArrayList<String> schema, long sampleSize, long datasetSize)
	{
		if (aggFunc.equalsIgnoreCase("COUNT") || aggFunc.equalsIgnoreCase("SUM") )
			return "SELECT approx_" + aggFunc + "_clean("+tableOperator.attrDifference(sampleName+"_dirty ",sampleName+"_clean ",attribute)+ " , " + tableOperator.accessAttr(sampleName+"_clean", "dup")+" , "+sampleSize +" , "+datasetSize+") " + tableOperator.rightOuterEquiJoin(sampleName+"_dirty",sampleName+"_clean","hash") + tableOperator.wherePredicate(predicate,sampleName + "_clean",schema) + tableOperator.groupBy(groupBy,sampleName + "_clean",schema);
		else
			return "SELECT approx_" + aggFunc + "_clean("+tableOperator.attrDifference(sampleName+"_dirty ",sampleName+"_clean ",attribute)+" , "+ tableOperator.accessAttr(sampleName+"_clean", "dup")+") " + tableOperator.rightOuterEquiJoin(sampleName+"_dirty",sampleName+"_clean","hash") + tableOperator.wherePredicate(predicate,sampleName + "_clean",schema) + tableOperator.groupBy(groupBy,sampleName + "_clean",schema);
	}

}
package org.apache.hadoop.hive.ql.sampleclean;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class SampleCleanSQLExtensionParser{

	private static HashMap<String,ArrayList<String>> schemaManager;

	private SampleCleanQueryBuilder scQueryBuilder;
	private SampleCleanOutlierRemoval outlierRemoval;
	private SampleCleanTextTransformations textTransform;

	private HiveConf conf;

	public long datasetSize = 0;
	public long sampleSize = 0;

	private static final int INIT_QUERY = 0;
	private static final String INIT_QUERY_KEYWORD = "scinitialize";

	private static final int RESET_QUERY = 1;
	private static final String RESET_QUERY_KEYWORD = "screset";

	private static final int SCAGG_QUERY = 2;
	private static final String SCAGG_QUERY_KEYWORD = "selectrawsc";

	private static final int OUTLIER_QUERY = 3;
	private static final String OUTLIER_QUERY_KEYWORD = "scoutlier";

	private static final int TEXT_FORMAT_QUERY = 4;
	private static final String TEXT_FORMAT_QUERY_KEYWORD = "scformat";

	private static final int BCAGG_QUERY = 5;
	private static final String BCAGG_QUERY_KEYWORD = "selectnsc";

	private static final int REMOVE_QUERY = 6;
	private static final String REMOVE_QUERY_KEYWORD = "scfilter";

	private static final int SELECT_QUERY = 7;
	private static final String SELECT_QUERY_KEYWORD = "scshow";

	//private static final int FORK_QUERY = 6;
	//private static final String FORK_QUERY_KEYWORD = "fork";


	public SampleCleanSQLExtensionParser(long datasetSize,long sampleSize)
	{
		this.datasetSize = datasetSize;
		this.sampleSize = sampleSize;

		schemaManager = new HashMap<String,ArrayList<String>>();
		scQueryBuilder = new SampleCleanQueryBuilder();
		outlierRemoval = new SampleCleanOutlierRemoval();
		textTransform = new SampleCleanTextTransformations();
	}

	public void setDatasetSize(long datasetSize)
	{
		this.datasetSize = datasetSize;
	}

	public void setSampleSize(long sampleSize)
	{
		this.sampleSize = sampleSize;
	}

	public void setHiveConf(HiveConf conf)
	{
		this.conf = conf;
	}

	public ArrayList<String> parse(String scQuery)
	{
		Scanner queryScanner = new Scanner(scQuery);
		String firstToken = queryScanner.next();
		//try{
			return exec(classifyQuery(firstToken), queryScanner);
		//}
		//catch(Exception e)
		//{
		//	return null;
		//}
	}

	public int classifyQuery(String firstToken)
	{
		firstToken = firstToken.trim().toLowerCase();
		if (firstToken.equals(INIT_QUERY_KEYWORD))
			return INIT_QUERY;
		else if (firstToken.equals(RESET_QUERY_KEYWORD))
			return RESET_QUERY;
		else if (firstToken.equals(SCAGG_QUERY_KEYWORD))
			return SCAGG_QUERY;
		else if (firstToken.equals(OUTLIER_QUERY_KEYWORD))
			return OUTLIER_QUERY;
		else if (firstToken.equals(TEXT_FORMAT_QUERY_KEYWORD))
			return TEXT_FORMAT_QUERY;
		else if (firstToken.equals(BCAGG_QUERY_KEYWORD))
			return BCAGG_QUERY;
		else if (firstToken.equals(REMOVE_QUERY_KEYWORD))
			return REMOVE_QUERY;
		else if (firstToken.equals(SELECT_QUERY_KEYWORD))
			return SELECT_QUERY;
		else 
			return -1;	
	}

	public ArrayList<String> exec(int query, Scanner queryScannerRemainingTokens)
	{
		String viewName = queryScannerRemainingTokens.next();
		String materializedScanner = materializeScanner(queryScannerRemainingTokens);
		switch(query)
		{
			case INIT_QUERY: return execInitQuery(viewName, parseInitQuery(new Scanner(materializedScanner)));
			case RESET_QUERY: return execResetQuery(viewName);
			case SCAGG_QUERY: return execRawSC(viewName + " " + materializedScanner);
			case OUTLIER_QUERY: return execOutlierQuery(viewName, parseOutlierQuery(new Scanner(materializedScanner)));
			case TEXT_FORMAT_QUERY: return execTextQuery(viewName, parseTextQuery(new Scanner(materializedScanner)));
			case BCAGG_QUERY: return execNormalizedSC(viewName + " " + materializedScanner);
			case REMOVE_QUERY: return execRemove(viewName, materializedScanner);
			case SELECT_QUERY: return execSelect(viewName, materializedScanner);
			default: return null;
		}
	}

	public String materializeScanner(Scanner queryScannerRemainingTokens)
	{
		String tmp ="";
		while (queryScannerRemainingTokens.hasNext())
			tmp += queryScannerRemainingTokens.next() + " ";
		return tmp;
	}

	public ArrayList<String> execRemove(String viewName, String predicate)
	{
		viewName = viewName + "_clean";
		String query_template = "INSERT OVERWRITE TABLE %v SELECT * FROM %v WHERE " + predicate;
		ArrayList<String> commandList = new ArrayList<String>();
		commandList.add(query_template.replace("%v", viewName));
		return commandList;
	}

	public ArrayList<String> execSelect(String viewName, String predicate)
	{
		viewName = viewName + "_clean";
		String query_template = "SELECT * FROM %v ";
		if (predicate.length() > 0)
			query_template += " WHERE " + predicate;
		ArrayList<String> commandList = new ArrayList<String>();
		commandList.add(query_template.replace("%v", viewName));
		return commandList;
	}

	public ArrayList<String> execRawSC(String queryText)
	{
		ArrayList<String> commandList = new ArrayList<String>();
		SampleCleanAggQueryParse scParse = new SampleCleanAggQueryParse(queryText);
		commandList.add(scQueryBuilder.rawSCQuery(scParse.getViewName(), scParse.getAggFunction(), scParse.getAttr(), scParse.getPredicate(), scParse.getGroupBy(), schemaManager.get(scParse.getViewName()),sampleSize, datasetSize));
		return commandList;
	}

	public ArrayList<String> execNormalizedSC(String queryText)
	{
		ArrayList<String> commandList = new ArrayList<String>();
		SampleCleanAggQueryParse scParse = new SampleCleanAggQueryParse(queryText);
		commandList.add(scQueryBuilder.normalizedSCQuery(scParse.getViewName(), scParse.getAggFunction(), scParse.getAttr(), scParse.getPredicate(), scParse.getGroupBy(),schemaManager.get(scParse.getViewName()), sampleSize, datasetSize));
		return commandList;
	}

	public ArrayList<String> execInitQuery(String viewName, ArrayList<String> schemaAndBT)
	{
		String baseTable = schemaAndBT.remove(schemaAndBT.size()-2);
		double samplingProb = Double.parseDouble(schemaAndBT.remove(schemaAndBT.size()-1));
		ArrayList<String> commandList = new ArrayList<String>();
		schemaManager.put(viewName,schemaAndBT);
		commandList.add(scQueryBuilder.createSample(viewName,baseTable,schemaAndBT,samplingProb));
		commandList.add(scQueryBuilder.createEmptyCleanTable(viewName,baseTable,schemaAndBT));
		commandList.add(scQueryBuilder.copyDirtyToClean(viewName));
		return commandList;
	}

	public ArrayList<String> parseInitQuery(Scanner queryScannerRemainingTokens)
	{
		String schemaString = "";
		while(queryScannerRemainingTokens.hasNext())
		{
			String nextToken = queryScannerRemainingTokens.next();

			if(nextToken.equalsIgnoreCase("from"))
				break;
			else
				schemaString += nextToken;
		}
			
		ArrayList<String> schema = new ArrayList<String>(Arrays.asList(schemaString.replace(")","").replace("(","").split(",")));
		schema.add(queryScannerRemainingTokens.next());

		//WITHSAMPLING
		queryScannerRemainingTokens.next();

		String samplingProb = queryScannerRemainingTokens.next();	
		schema.add(samplingProb);

		return schema;
	}

	public ArrayList<String> execResetQuery(String viewName)
	{
		ArrayList<String> commandList = new ArrayList<String>();
		commandList.add(scQueryBuilder.copyDirtyToClean(viewName));
		return commandList;
	}

	public ArrayList<String> execOutlierQuery(String viewName, ArrayList<String> args)
	{
		String attr = args.get(0);
		String method = args.get(1);
		String alpha = args.get(2);
		ArrayList<String> commandList = new ArrayList<String>();
		commandList.addAll(outlierRemoval.buildOutlierRemovalQuery(viewName, method, attr, alpha));
		return commandList;
	}

	public ArrayList<String> parseOutlierQuery(Scanner queryScannerRemainingTokens)
	{
		ArrayList<String> argsList = new ArrayList<String>();
		argsList.add(queryScannerRemainingTokens.next());
		argsList.add(queryScannerRemainingTokens.next());
		argsList.add(materializeScanner(queryScannerRemainingTokens));
		return argsList;
	}

	public ArrayList<String> execTextQuery(String viewName, ArrayList<String> args)
	{
		ArrayList<String> commandList = new ArrayList<String>();

		try{
		HiveMetaStoreClient msc = new HiveMetaStoreClient(conf);
		StorageDescriptor sd = msc.getTable(viewName+"_clean").getSd();
		List<FieldSchema> fieldSchema = sd.getCols();
		ArrayList<String> schemaList = new ArrayList<String>();

		for (FieldSchema field: fieldSchema)
			schemaList.add(field.getName());

        //String tableColString = msc.getTable(viewName+"_clean").toString();

		commandList.add(textTransform.buildTextFormatQuery(viewName, schemaList,args));
		}
		catch(Exception metaException){}

		return commandList;
	}

	public ArrayList<String> parseTextQuery(Scanner queryScannerRemainingTokens)
	{
		ArrayList<String> argsList = new ArrayList<String>();
		argsList.add(queryScannerRemainingTokens.next());
		argsList.add(queryScannerRemainingTokens.next());
		if (queryScannerRemainingTokens.hasNext())
			{
				argsList.add(queryScannerRemainingTokens.next());
				argsList.add(materializeScanner(queryScannerRemainingTokens));
			}
		return argsList;
	}
}
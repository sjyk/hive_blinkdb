package org.apache.hadoop.hive.ql.sampleclean;

public class SampleCleanAggQueryParse
{
	String queryString = "";
	public SampleCleanAggQueryParse(String queryString)
	{
		this.queryString = queryString.toLowerCase();
	}

	public String getAggFunction()
	{
		int cut = queryString.indexOf("(");
		String agg = queryString.substring(0,cut);
		return agg.trim();
	}

	public String getAttr()
	{
		int cut1 = queryString.indexOf("(");
		int cut2 = queryString.indexOf(")");
		String agg = queryString.substring(cut1+1,cut2);
		return agg.trim();
	}

	public String getViewName()
	{
		int cut1 = queryString.indexOf("from");
		int cut2 = queryString.indexOf("where");
		if (cut2 == -1)
		{
			cut2 = queryString.length();
			cut2 = queryString.indexOf("group by");
			if (cut2 == -1)
				cut2 = queryString.length();
		}
		String agg = queryString.substring(cut1+4,cut2);
		return agg.trim();
	}

	public String getPredicate()
	{
		int cut1 = queryString.indexOf("where");
		
		if(cut1 == -1)
			return "";

		int cut2 = queryString.indexOf("group by");

		if (cut2 == -1)
			return queryString.substring(cut1+5);

		String agg = queryString.substring(cut1+5,cut2);
		return agg.trim();
	}

	public String getGroupBy()
	{
		int cut1 = queryString.indexOf("group by");
		
		if(cut1 == -1)
			return "";

		String agg = queryString.substring(cut1+8);
		return agg.trim();
	}
}

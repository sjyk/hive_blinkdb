package org.apache.hadoop.hive.ql.sampleclean;
import java.util.ArrayList;

public class SampleCleanUnitTest {

	private static final String CREATE_SAMPLE = "CREATE TABLE tweets_sample_dirty AS SELECT v.* from tweets LATERAL VIEW clean_export(id, user, tweet) v AS hash, dup, id, user, tweet WITHSAMPLING 0.1";
	private static final String CREATE_CLEAN = "CREATE TABLE tweets_sample_clean(hash string, dup int, id string, user string, tweet string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";
	private static final String COPY_DIRTY = "INSERT OVERWRITE TABLE tweets_sample_clean SELECT * FROM tweets_sample_dirty";

	public static void main(String [] args)
	{
		SampleCleanQueryBuilder sampleClean = new SampleCleanQueryBuilder();
		SampleCleanOutlierRemoval outlierRemoval = new SampleCleanOutlierRemoval();
		SampleCleanTextTransformations textTransformations = new SampleCleanTextTransformations();
		System.out.println("[1] Successfully initialized SampleCleanQueryBuilder Object\n");

		//Create a demo table
		System.out.println("[2] Creating a sample");
		ArrayList<String> demoTable = new ArrayList<String>();
		demoTable.add("id");
		demoTable.add("user");
		demoTable.add("tweet");
		System.out.println("Demo Table: tweets " + demoTable);

		//create sample
		System.out.println("HIVEQL: " + sampleClean.createSample("tweets_sample", "tweets", demoTable, 0.1));
		System.out.println("Created Sample: " + CREATE_SAMPLE.equals(sampleClean.createSample("tweets_sample", "tweets", demoTable, 0.1)));
		System.out.println("");

		//create empty clean table
		System.out.println("[3] Creating empty clean table");
		System.out.println("HIVEQL: " + sampleClean.createEmptyCleanTable("tweets_sample", "tweets", demoTable));
		System.out.println("Created Clean: " + CREATE_CLEAN.equals(sampleClean.createEmptyCleanTable("tweets_sample", "tweets", demoTable)));
		System.out.println("");


		System.out.println("[4] Initializing clean table with dirty data");
		System.out.println("HIVEQL: " + sampleClean.copyDirtyToClean("tweets_sample"));
		System.out.println("Initialized Clean Table: " + COPY_DIRTY.equals(sampleClean.copyDirtyToClean("tweets_sample")));
		System.out.println("");

		System.out.println("[5] RawSC Count Tweets from user a");
		System.out.println("HIVEQL: " + sampleClean.rawSCQuery("tweets_sample","count","1","user = \"a\"","",demoTable,100,1000));
		System.out.println("");


		System.out.println("[6] NormalizedSC Sum Tweet Ids from user a");
		System.out.println("HIVEQL: " + sampleClean.normalizedSCQuery("tweets_sample","sum","id","user = \"a\"","",demoTable,100,1000));
		System.out.println("");

		System.out.println("[7] Outlier removal on id");
		System.out.println("HIVEQL: " + outlierRemoval.buildOutlierRemovalQuery("tweets_sample",outlierRemoval.NORM,"id","1.96"));
		System.out.println("");

		System.out.println("[8] Text Replacement");
		System.out.println("HIVEQL: " + textTransformations.replaceQuery("tweets_sample",demoTable,"tweet","S","s"));
		System.out.println("");

		SampleCleanSQLExtensionParser parser = new SampleCleanSQLExtensionParser(100,400);
		System.out.println("[9.1] Parser Tests");
		System.out.println(parser.parse("SCINITIALIZE tweets_sample (id,user,tweet) from tweets withsampling 0.1"));
		System.out.println("");

		System.out.println("[9.2] Parser Tests");
		System.out.println(parser.parse("SCRESET tweets_sample"));
		System.out.println("");

		System.out.println("[9.3] Parser Tests");
		System.out.println(parser.parse("SCOUTLIER tweets_sample id parametric 1.96"));
		System.out.println("");

		System.out.println("[9.4] Parser Tests");
		System.out.println(parser.parse("SCFORMAT tweets_sample id number"));
		System.out.println(parser.parse("SCFORMAT tweets_sample user trim"));
		System.out.println(parser.parse("SCFORMAT tweets_sample tweet replace \':)\' \':(\' "));
		System.out.println("");

		System.out.println("[9.5] Parser Tests");
		SampleCleanAggQueryParse scParse = new SampleCleanAggQueryParse("sum(id) from tweets_sample where user = 'tim' and id > 100 group by tweet");
		System.out.println(scParse.getAttr());
		System.out.println(scParse.getPredicate());
		System.out.println(scParse.getAggFunction());
		System.out.println(scParse.getViewName());


		System.out.println("[9.6] Parser Tests");
		System.out.println(parser.parse("SELECTRAWSC sum(id) from tweets_sample where user = 'tim' and id > 100 group by tweet"));
		System.out.println(parser.parse("SELECTRAWSC count(1) from tweets_sample"));
		System.out.println(parser.parse("SELECTNSC sum(id) from tweets_sample where user = 'tim' and id > 100 group by tweet"));
		System.out.println(parser.parse("SELECTNSC count(1) from tweets_sample"));
		System.out.println("");
 
	}

}

package org.apache.hadoop.hive.ql.sampleclean;

import java.util.ArrayList;
import java.util.Collection;

public class SampleCleanOutlierRemoval {

	public static final String MAD = "nonparametric";
    public static final String NORM = "parametric";
    public static final String PROX = "proximity";

    private static final String MAD_QUERY = "SELECT %v.hash AS hash, ROUND(ABS(%v.%a - t.med)) AS z FROM %v JOIN (SELECT PERCENTILE(ROUND(%a),0.5) AS med FROM %v) t on TRUE";
    private static final String NORM_QUERY = "SELECT %v.hash AS hash, ABS(%v.%a - t.mu)/t.sigma AS z FROM %v JOIN (SELECT AVG(%a) AS mu ,STD(%a) AS sigma FROM %v) t ON TRUE";
    private static final String PROX_QUERY = "SELECT %v.hash AS hash, percentile(round(abs(%v.%a-t.%a)),0.25)/std(t.%a) as z FROM %v JOIN (select * from %v) t ON TRUE GROUP BY %v.hash";

    private static final String NORM_THRESHOLD = "SELECT %p as thresh FROM %v_score LIMIT 1";
    private static final String PROX_THRESHOLD = "SELECT %p as thresh FROM %v_score LIMIT 1";
    private static final String MAD_THRESHOLD = "SELECT %p*PERCENTILE(z,0.5) as thresh FROM %v_score LIMIT 1";

    private static final String FILTER_QUERY = "SELECT t.hash from (SELECT * FROM %v_score JOIN %v_threshold ON TRUE) t WHERE t.z <= t.thresh";
    private static final String MERGE_BACK = "INSERT OVERWRITE TABLE %v SELECT * FROM %v LEFT SEMI JOIN %v_inliers on (%v.hash = %v_inliers.hash)";
    //private static final String RULE_QUERY = "INSERT OVERWRITE TABLE %v SELECT * FROM %v WHERE ";

    public String scoreQuery(String view, String attribute, String method)
    {
        String query = "";
        if (method.equals(MAD))
            query = MAD_QUERY;
        else if (method.equals(NORM))
            query = NORM_QUERY;
        else if (method.equals(PROX))
            query = PROX_QUERY;

        return "CREATE TABLE " + view+"_score" + " AS " + query.replaceAll("%v",view).replaceAll("%a",attribute);
    }


    public String threshQuery(String view, String alpha, String method)
    {
        String query = "";
        if (method.equals(MAD))
            query = MAD_THRESHOLD;
        else if (method.equals(NORM))
            query = NORM_THRESHOLD;
        else if (method.equals(PROX))
            query = PROX_THRESHOLD;

        return "CREATE TABLE " + view+"_threshold" + " AS " + query.replaceAll("%v",view).replaceAll("%p", alpha);
    }

    public String joinAndFilter(String view)
    {

        return "CREATE TABLE " + view+"_inliers" + " AS " + FILTER_QUERY.replaceAll("%v",view);
    }

    public String merge(String view)
    {
        return MERGE_BACK.replaceAll("%v",view);
    }

    //public String ruleQuery(String view, String rule)
    //{
    //    return RULE_QUERY.replaceAll("%v",view) + " " + rule;
    //}

    public ArrayList<String> cleanup(String view)
    {
        ArrayList<String> commandList = new ArrayList<String>();
        commandList.add("DROP TABLE " + view+"_score");
        commandList.add("DROP TABLE " + view+"_threshold");
        commandList.add("DROP TABLE " + view+"_inliers");
        return commandList;
    }

    public ArrayList<String> buildOutlierRemovalQuery(String view, String method, String attr, String alpha)
    {
    	ArrayList<String> commandList = new ArrayList<String>();
    	view = view + "_clean";
    	commandList.add(scoreQuery(view,attr,method));
        commandList.add(threshQuery(view, alpha, method));
        commandList.add(joinAndFilter(view));
    	commandList.addAll(cleanup(view));
        return commandList;
    }

}

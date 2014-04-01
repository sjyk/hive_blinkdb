package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Outlier Detection Query Rewriter
 * We execute outlier removal in SQL, this module dynamically
 * generates the sql.
 * SampleClean Query Rewriting Module.
 * Sanjay Krishnan, Jiannan Wang, Sameer Agarwal
 */
public class OutlierDetectRewriter {

    public static final String MAD = "mad";
    public static final String NORM = "z";

    private static final String MAD_QUERY = "SELECT %v.hash AS hash, ROUND(ABS(%v.%a - t.med)) AS z FROM %v JOIN (SELECT PERCENTILE(ROUND(%a),0.5) AS med FROM %v) t on TRUE";
    private static final String NORM_QUERY = "SELECT %v.hash AS hash, ABS(%v.key - t.mu)/t.sigma AS z FROM %v JOIN (SELECT AVG(%a) AS mu ,STD(%a) AS sigma FROM %v) t ON TRUE";

    private static final String NORM_THRESHOLD = "SELECT %p as thresh FROM %v_score LIMIT 1";
    private static final String MAD_THRESHOLD = "SELECT %p*PERCENTILE(z,0.5) as thresh FROM %v_score LIMIT 1";

    private static final String FILTER_QUERY = "SELECT t.hash from (SELECT * FROM %v_score JOIN %v_threshold ON TRUE) t WHERE t.z <= t.thresh";
    private static final String MERGE_BACK = "INSERT OVERWRITE TABLE %v SELECT * FROM %v LEFT SEMI JOIN %v_inliers on (%v.hash = %v_inliers.hash)";

    public String scoreQuery(String view, String attribute, String method)
    {
        String query = "";
        if (method.equals(MAD))
            query = MAD_QUERY;
        else if (method.equals(NORM))
            query = NORM_QUERY;

        return "CREATE TABLE " + view+"_score" + " AS " + query.replaceAll("%v",view).replaceAll("%a",attribute);
    }


    public String threshQuery(String view, String alpha, String method)
    {
        String query = "";
        if (method.equals(MAD))
            query = MAD_THRESHOLD;
        else if (method.equals(NORM))
            query = NORM_THRESHOLD;

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

    public ArrayList<String> cleanup(String view)
    {
        ArrayList<String> commandList = new ArrayList<String>();
        commandList.add("DROP TABLE " + view+"_score");
        commandList.add("DROP TABLE " + view+"_threshold");
        commandList.add("DROP TABLE " + view+"_inliers");
        return commandList;
    }

}

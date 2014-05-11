package org.apache.hadoop.hive.ql.sampleclean;

import java.util.ArrayList;
import java.util.Collection;

public class SampleCleanTextTransformations {

    private static final String BASE_QUERY = " SELECT %s from %v ";
    private static final String UPDATE_QUERY = " INSERT OVERWRITE TABLE %v ";
 
    private static final String NUMBER_FORMAT = "regexp_extract(trim(lower(%a)),'[0-9]+',0) ";
    private static final String TRIM_QUERY = " trim(%a) ";
    private static final String REPLACE_QUERY = " regexp_replace(%a,%rarg1,%rarg2) ";

    public String numberFormatQuery(String view, ArrayList<String> schemaList, String attribute)
    {
        String query = UPDATE_QUERY + " " + BASE_QUERY;
        String schema = SampleCleanHiveTableOps.schemaToString(schemaList);
        return query.replace("%s",schema).replace("%v",view+"_clean").replace(" "+attribute+" ",NUMBER_FORMAT).replace("%a",attribute);
    }

    public String trimQuery(String view, ArrayList<String> schemaList, String attribute)
    {
    	String query = UPDATE_QUERY + " " + BASE_QUERY;
    	String schema = SampleCleanHiveTableOps.schemaToString(schemaList);
        return query.replace("%s",schema).replace("%v",view+"_clean").replace(" "+attribute+" ",TRIM_QUERY).replace("%a",attribute);
    }

    public String replaceQuery(String view, ArrayList<String> schemaList, String attribute, String in, String out)
    {
    	String query = UPDATE_QUERY + " " + BASE_QUERY;
    	String schema = SampleCleanHiveTableOps.schemaToString(schemaList);
        String paramQuery = query.replace("%s",schema).replace("%v",view+"_clean").replace(" "+attribute+" ",REPLACE_QUERY).replace("%a",attribute);
        return paramQuery.replace("%rarg1",in).replace("%rarg2",out);
    }

    public String buildTextFormatQuery(String view, ArrayList<String> schemaList, ArrayList<String> args)
    {
        String attr = args.get(0);
        String command = args.get(1);
        if (command.equalsIgnoreCase("number"))
        {
            return numberFormatQuery(view, schemaList, attr);
        }
        else if (command.equalsIgnoreCase("trim"))
        {
            return trimQuery(view, schemaList, attr);
        }
        else if (command.equalsIgnoreCase("replace"))
        {
            return replaceQuery(view, schemaList, attr, args.get(2),args.get(3));
        }
        else
            return "";
    }

}

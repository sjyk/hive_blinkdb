package org.apache.hadoop.hive.ql.sampleclean;

import java.util.ArrayList;
import java.util.Collection;

public class SampleCleanHiveTableOps {

    public static String schemaToString(ArrayList<String> schema)
    {
        String result = "";
        for(int i=0;i<schema.size(); i++)
        {
            String attr = schema.get(i);

            if (i < schema.size() - 1)
                result += attr + " , ";
            else
                result += attr;
        }
        return result;
    }

    public String schemaToTypedString(ArrayList<String> schema, ArrayList<String> types)
    {
        String result = "";
        for(int i=0;i<schema.size(); i++)
        {
            String attr = schema.get(i);
            String type = types.get(i);

            if (i < schema.size() - 1)
                result += attr + " " + type + " , ";
            else
                result += attr + " " + type;
        }
        return result;
    }

    public String createTable(String tableName)
    {
        
        String cmd = "CREATE TABLE " + tableName;
        return cmd;
    }

    public String createTypedTable(String tableName, ArrayList<String> schema, ArrayList<String> types)
    {
        
        String cmd = createTable(tableName)+"("+ schemaToTypedString(schema,types)+")";
        return cmd;
    } 

    public String createTypedTableWithCSV(String tableName, ArrayList<String> schema, ArrayList<String> types)
    {
        
        String cmd = createTable(tableName)+"("+ schemaToTypedString(schema,types)+") ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'";
        return cmd;
    } 

    public String createTableAs(String tableName, String query)
    {
        
        String cmd = createTable(tableName) + asQuery(query);
        return cmd;
    }

    public String materializeUDTFView(String udtf, String baseTable, ArrayList<String> schema, ArrayList<String> preSchema)
    {
        preSchema.addAll(schema);
        String cmd = "SELECT v.* from " + baseTable + " LATERAL VIEW " + udtf + "(" + schemaToString(schema) + ") v AS " + schemaToString(preSchema); 
        return cmd;
    }   

    public String copyTableTo(String src, String dest)
    {
        String cmd = "INSERT OVERWRITE TABLE " + dest +" SELECT * FROM " + src;
        return cmd;
    }

    public String asQuery(String query)
    {
        return " AS " + query + " ";
    }

    public String wherePredicate(String predicate)
    {
        if (predicate.trim().length() > 0)
            return " WHERE " + predicate + " ";
        else
            return " ";
    }

    public String wherePredicate(String predicate, String table, ArrayList<String> schema)
    {
        if (predicate.trim().length() > 0)
            return " WHERE " + clarifyAllAtributes(table,schema, predicate) + " ";
        else
            return " ";
    }

    public String groupBy(String groupBy)
    {
        if (groupBy.trim().length() > 0)
            return " GROUP BY " + groupBy + " ";
        else
            return " ";
    }

    public String groupBy(String groupBy, String table, ArrayList<String> schema)
    {
        if (groupBy.trim().length() > 0)
            return " GROUP BY " + clarifyAllAtributes(table,schema, groupBy) + " ";
        else
            return " ";
    }

    public String accessAttr(String table, String attr)
    {
        return " " + table +"." +attr + " ";
    }

    public String coalescedAccessAttr(String table, String attr)
    {
        return " coalesce(" + table +"." +attr + "-0,0) ";
    }

    public String clarifyAllAtributes(String table, ArrayList<String> schema, String queryText)
    {
        queryText = " " + queryText + " ";

        for(String attr: schema)
        {
            queryText = queryText.replace(" "+attr.trim()+" ",accessAttr(table,attr));
        }

        return queryText;
    }

    public String attrDifference(String table1, String table2, String attr)
    {
        return coalescedAccessAttr(table1,attr) + " - " + coalescedAccessAttr(table2,attr);
    }

    public String withSampling(double samplingProb)
    {
        return " SAMPLEWITH " + samplingProb;
    }

    public String predicateFilter(String src, String dest, String predicate)
    {
        String cmd = copyTableTo(src, dest) + wherePredicate(predicate);
        return cmd;
    }

    public String semiJoinFilter(String src, String dest, String joinTable, String joinAttr)
    {
        String cmd = copyTableTo(src, dest) + " LEFT SEMI JOIN ";
        cmd += joinTable + " on (" + src + "." + joinAttr +" = " + joinTable + "." + joinAttr +")";
        return cmd;
    }

    public String exportToFile(String table, String fileName)
    {
        String cmd = "INSERT OVERWRITE DIRECTORY \'" + fileName +"\' SELECT * FROM " + table;
        return cmd;
    }

    public String importFromFile(String table, String fileName)
    {
        String cmd = "LOAD DATA LOCAL INPATH \'" +fileName+"\' INTO TABLE " + table;
        return cmd;
    }

    public String rightOuterEquiJoin(String table1, String table2, String attr)
    {
        return " FROM " + table1 + " RIGHT OUTER JOIN " + table2 + " ON (" + accessAttr(table1,attr) + " = " + accessAttr(table2,attr) + ") ";
    }
}

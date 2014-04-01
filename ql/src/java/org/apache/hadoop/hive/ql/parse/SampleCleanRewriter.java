package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * SampleClean Query Rewriting Module.
 * Sanjay Krishnan, Jiannan Wang, Sameer Agarwal
 * This class defines basic methods to rewrite HiveQL
 * Queries (with some additional SampleClean syntax)
 * into queries that can be executed.
 *
 * We define the following newqueries
 *
 * 1. CREATE CLEAN VIEW <viewname> AS <query>
 * 2. MERGE CLEAN VIEW <viewname>
 *
 * Behind the scenes these queries are handled as
 * a separate _clean and _dirty table.
 */
public class SampleCleanRewriter {

    //setup logging for this module
    private static final Log LOG = LogFactory.getLog("hive.ql.parse.SampleCleanRewriter");

    //reserved words
    private static final String CLEAN_COMMAND = "clean";
    private static final String CREATE_COMMAND = "create";
    private static final String MERGE_COMMAND = "merge";
    private static final String COPY_COMMAND = "copy";
    private static final String OUTLIER_COMMAND = "outlier";

    private static final String RAWSC = "rawsc";
    private static final String NORMALIZEDSC = "normalizedsc";

    //HQL syntax
    private static final String HIVEQL_LOAD_TABLE = "LOAD DATA LOCAL INPATH";
    private static final String HIVEQL_CREATE_TABLE = "CREATE TABLE";
    private static final String HIVEQL_TABLE_STORAGE_DEFAULT = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' AS SELECT t.* FROM";
    private static final String HIVEQL_JOIN_TABLE = "RIGHT OUTER JOIN";
    private static final String HIVEQL_SAMPLECLEAN_UDF = "clean_export";
    private static final String HIVEQL_HASH_COL_NAME = "hash";
    private static final String HIVEQL_DUP_COL_NAME = "dup";
    private static final String HIVEQL_CLEAN_TABLE_STORAGE = "ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\'";
    private static final String HIVEQL_OVERWRITE_DIRECTORY = "INSERT OVERWRITE DIRECTORY";
    private static final String HIVEQL_OVERWRITE_TABLE = "INSERT OVERWRITE TABLE";
    private static final String HIVEQL_COPY_TABLE = "SELECT * FROM";

    //class variables
    private String defaultOutFile = "";
    private String defaultInFile = "";
    private String scMode = "";

    /*
    @param defaultOutFile Default output file typically in HDFS
    @param defaultInFile Default in file typically on the driver
    @param scMode "RawSC" or "NormalizedSC", read our paper to figure out which one you should use.
     */
    public SampleCleanRewriter(String defaultOutFile, String defaultInFile, String scMode)
    {
        this.defaultOutFile = defaultOutFile;
        this.defaultInFile = defaultInFile;
        this.scMode = scMode;
    }

    /*  Rewrite is the main driver of the SampleClean framework, it takes in a HiveQL command
        and returns a list of commands to execute. This is a one to many rewriting.
        @param command
     */
    public ArrayList<String> rewrite(String command) throws SampleCleanSyntaxException
    {
        ArrayList<String> commandList = new ArrayList<String>();

        if (! command.toLowerCase().contains(" "+ CLEAN_COMMAND+ " "))
        {
            return commandList; //if the key word clean is not in the command return an empty list
        }

        //If its a create or a merge handle differently, else apply standard query re-writing
        if (command.toLowerCase().startsWith(CREATE_COMMAND))
            return dirtyViewRewrite(command);
        else if (command.toLowerCase().startsWith(MERGE_COMMAND))
            return mergeViewRewrite(command);
        else if (command.toLowerCase().startsWith(COPY_COMMAND))
            return copyViewRewrite(command);
        else if (command.toLowerCase().startsWith(OUTLIER_COMMAND))
            return outlierViewRewrite(command);
        else if (scMode.equalsIgnoreCase(RAWSC))
            return queryRewrite(command);
        else if (scMode.equalsIgnoreCase(NORMALIZEDSC))
            return correctionQueryRewrite(command);
        else
            throw new SampleCleanSyntaxException("SampleClean: Illegal Operating Mode!");
    }

    private ArrayList<String> mergeViewRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> initialTokens = tokenizeQuery(queryScanner);

        ArrayList<String> commandList = new ArrayList<String>();

        String result = HIVEQL_LOAD_TABLE + " \'" +defaultInFile+"\' INTO TABLE ";

        if (isValidMergeQuery(initialTokens.get(0),
                initialTokens.get(1),
                initialTokens.get(2),
                initialTokens.get(3)))
        {
            commandList.add(result+initialTokens.get(3)+"_clean");
        }

        return commandList;

    }

    private ArrayList<String> copyViewRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> initialTokens = tokenizeQuery(queryScanner);

        ArrayList<String> commandList = new ArrayList<String>();

        String result = HIVEQL_OVERWRITE_TABLE + " ";

        if (isValidCopyQuery(initialTokens.get(0),
                initialTokens.get(1),
                initialTokens.get(2),
                initialTokens.get(3)))
        {
            commandList.add(result+initialTokens.get(3)+"_clean " + HIVEQL_COPY_TABLE +" " +  initialTokens.get(3)+"_dirty" );
        }

        return commandList;

    }

    private ArrayList<String> outlierViewRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> initialTokens = tokenizeQuery(queryScanner);
        OutlierDetectRewriter ow = new OutlierDetectRewriter();

        ArrayList<String> commandList = new ArrayList<String>();

        String result = "";

        if (isValidOutlierQuery(initialTokens.get(0),
                initialTokens.get(1),
                initialTokens.get(2),
                initialTokens.get(3)))
        {
            String attr = "";
            if (!queryScanner.hasNext())
            {
                throw new SampleCleanSyntaxException("SampleClean: Specify an attribute with outlier");
            }
            attr = queryScanner.next();

            String method = "";
            if (!queryScanner.hasNext())
            {
                throw new SampleCleanSyntaxException("SampleClean: Specify a method (parametric, nonparametric)");
            }
            method = queryScanner.next();

            if(method.toLowerCase().equals("nonparametric"))
            {
                method = OutlierDetectRewriter.MAD;
            }
            else if(method.toLowerCase().equals("parametric"))
            {
                method = OutlierDetectRewriter.NORM;
            }
            else
            {
                throw new SampleCleanSyntaxException("SampleClean: Unknown outlier method!");
            }

            String alpha = "2.57";
            if (queryScanner.hasNext())
                alpha = queryScanner.next();

            commandList.add(ow.scoreQuery(initialTokens.get(3)+"_clean",attr,method));
            commandList.add(ow.threshQuery(initialTokens.get(3) + "_clean", alpha, method));
            commandList.add(ow.joinAndFilter(initialTokens.get(3) + "_clean"));
            commandList.add(ow.merge(initialTokens.get(3)+"_clean"));
            commandList.addAll(ow.cleanup(initialTokens.get(3)+"_clean"));
        }

        return commandList;

    }

    private ArrayList<String> dirtyViewRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> initialTokens = tokenizeQuery(queryScanner);

        ArrayList<String> commandList = new ArrayList<String>();

        String result = HIVEQL_CREATE_TABLE + " "; //create dirty table

        if (isValidCreateQuery(initialTokens.get(0),
                initialTokens.get(1),
                initialTokens.get(2),
                initialTokens.get(3)))
        {
            result += initialTokens.get(3) + "_dirty "+ HIVEQL_TABLE_STORAGE_DEFAULT +" "; //set format for the dirty table

            ArrayList<String> sourceAndSchemaTokens = getUDFRewrite(queryScanner);
            result += sourceAndSchemaTokens.get(0); //set schema and execute our udf

            String remainingTokens = "";
            while(queryScanner.hasNext())
                remainingTokens += queryScanner.next().toLowerCase() + " ";

            result += remainingTokens;
            commandList.add(result); //add whatever predicate is there

            String cleanViewTable = HIVEQL_CREATE_TABLE + " " + initialTokens.get(3) + "_clean("+ sourceAndSchemaTokens.get(1)+" string)";
            cleanViewTable = cleanViewTable.replaceAll(","," string,") + HIVEQL_CLEAN_TABLE_STORAGE + " ";
            commandList.add(cleanViewTable); //create clean table

            String exportTable = HIVEQL_OVERWRITE_DIRECTORY + " \'" + defaultOutFile + "\' SELECT * FROM " + initialTokens.get(3) + "_dirty";
            commandList.add(exportTable); //export dirty table

            return commandList;
        }
        else
        {
            return commandList;
        }

    }

    /*
    Re-writes blinkdb syntax into sample clean syntax RawSC.
     */
    private ArrayList<String> queryRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> commandList = new ArrayList<String>();
        String hqlCommand = command.replaceAll(" "+ CLEAN_COMMAND+ " "," ") + " ";

        while(queryScanner.hasNext())
        {
            String currentToken = queryScanner.next().toLowerCase();

            if (currentToken.equals("from"))
                break;
        }

        String viewName = queryScanner.next();
        String cleanhqlCommand = hqlCommand.replaceAll(viewName, viewName + "_clean");

        int firstIndexOfApprox = cleanhqlCommand.indexOf("approx_sum");
        if(firstIndexOfApprox != -1)
        {
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",",dup)");
            cleanhqlCommand = cleanhqlCommand.replaceAll("approx_sum","approx_sum_clean");
        }

        firstIndexOfApprox = cleanhqlCommand.indexOf("approx_avg");
        if(firstIndexOfApprox != -1)
        {
        cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",",dup)");
        cleanhqlCommand = cleanhqlCommand.replaceAll("approx_avg","approx_avg_clean");
        }

        firstIndexOfApprox = cleanhqlCommand.indexOf("approx_count");
        if(firstIndexOfApprox != -1)
        {
        cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",",dup)");
        cleanhqlCommand = cleanhqlCommand.replaceAll("approx_count","approx_count_clean");
        }

        commandList.add(cleanhqlCommand);

        return commandList;
    }

    /*
   Re-writes blinkdb syntax into sample clean syntax NormalizedSC.
    */
    private ArrayList<String> correctionQueryRewrite(String command) throws SampleCleanSyntaxException
    {
        Scanner queryScanner = new Scanner(command);
        ArrayList<String> commandList = new ArrayList<String>();
        String hqlCommand = command.replaceAll(" "+ CLEAN_COMMAND+ " "," ") + " ";

        while(queryScanner.hasNext())
        {
            String currentToken = queryScanner.next().toLowerCase();

            if (currentToken.equals("from"))
                break;
        }

        String viewName = queryScanner.next();
        String cleanhqlCommand = hqlCommand.replaceAll(viewName, viewName + "_clean")+ " " + HIVEQL_JOIN_TABLE + " " + viewName + "_dirty";
        cleanhqlCommand += " ON (" + viewName + "_clean.hash=" + viewName + "_dirty.hash)";

        int firstIndexOfApprox = cleanhqlCommand.indexOf("approx_sum");
        if(firstIndexOfApprox != -1)
        {
            ArrayList<String> argReplacement = correctionArg(cleanhqlCommand.substring(firstIndexOfApprox), viewName);
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) +  cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst(argReplacement.get(0),argReplacement.get(1));

            firstIndexOfApprox = cleanhqlCommand.indexOf("approx_sum");
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",","+viewName+"_clean.dup)");
            cleanhqlCommand = cleanhqlCommand.replaceAll("approx_sum","approx_sum_clean");
        }

        firstIndexOfApprox = cleanhqlCommand.indexOf("approx_avg");
        if(firstIndexOfApprox != -1)
        {
            ArrayList<String> argReplacement = correctionArg(cleanhqlCommand.substring(firstIndexOfApprox),viewName);
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst(argReplacement.get(0),argReplacement.get(1));

            firstIndexOfApprox = cleanhqlCommand.indexOf("approx_avg");
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",","+viewName+"_clean.dup)");
            cleanhqlCommand = cleanhqlCommand.replaceAll("approx_avg","approx_avg_clean");
        }

        firstIndexOfApprox = cleanhqlCommand.indexOf("approx_count");
        if(firstIndexOfApprox != -1)
        {
            ArrayList<String> argReplacement = correctionArg(cleanhqlCommand.substring(firstIndexOfApprox),viewName);
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst(argReplacement.get(0),argReplacement.get(1));

            firstIndexOfApprox = cleanhqlCommand.indexOf("approx_count");
            cleanhqlCommand = cleanhqlCommand.substring(0,firstIndexOfApprox) + cleanhqlCommand.substring(firstIndexOfApprox).replaceFirst("\\)",","+viewName+"_clean.dup)");
            cleanhqlCommand = cleanhqlCommand.replaceAll("approx_count","approx_count_clean");
        }

        commandList.add(cleanhqlCommand);

        return commandList;
    }

    private ArrayList<String> correctionArg(String argSubString,String viewName)
    {
        ArrayList<String> rtnString = new ArrayList<String>();
        String argument = argSubString.substring(argSubString.indexOf("(")+1,argSubString.indexOf(")")).trim();
        rtnString.add(argument);
        rtnString.add(viewName + "_dirty." + argument + " - " + viewName + "_clean." + argument );
        return rtnString;
    }

    private ArrayList<String> countCorrectionArg(String argSubString,String viewName)
    {
        ArrayList<String> rtnString = new ArrayList<String>();
        String argument = argSubString.substring(argSubString.indexOf("(")+1,argSubString.indexOf(")")).trim();
        rtnString.add(argument);
        rtnString.add(viewName + "_clean." + argument );
        return rtnString;
    }

    /* Tests whether the merge query is prefaced with the right syntax
     */
    private boolean isValidMergeQuery(String preface, String type, String action, String viewName) throws SampleCleanSyntaxException
    {
        return (preface.equals(MERGE_COMMAND) &&
                type.equals(CLEAN_COMMAND) &&
                action.equals("view") &&
               (viewName.length() != 0 || !viewName.toLowerCase().equals("as")));
    }

    /* Tests whether the copy query is prefaced with the right syntax
     */
    private boolean isValidCopyQuery(String preface, String type, String action, String viewName) throws SampleCleanSyntaxException
    {
        return (preface.equals(COPY_COMMAND) &&
                type.equals(CLEAN_COMMAND) &&
                action.equals("view") &&
                (viewName.length() != 0 || !viewName.toLowerCase().equals("as")));
    }

    /* Tests whether the outlier query is prefaced with the right syntax
 */
    private boolean isValidOutlierQuery(String preface, String type, String action, String viewName) throws SampleCleanSyntaxException
    {
        return (preface.equals(OUTLIER_COMMAND) &&
                type.equals(CLEAN_COMMAND) &&
                action.equals("view") &&
                (viewName.length() != 0 || !viewName.toLowerCase().equals("as")));
    }

    /* Tests whether the create query is prefaced with the right syntax
     */
    private boolean isValidCreateQuery(String preface, String type, String action, String viewName) throws SampleCleanSyntaxException
    {
        return (preface.equals(CREATE_COMMAND) &&
                type.equals(CLEAN_COMMAND) &&
                action.equals("view") &&
                (viewName.length() != 0 || !viewName.toLowerCase().equals("as")));
    }

    /* Breaks the queries intial tokens into a list
     */
    private ArrayList<String> tokenizeQuery(Scanner queryScanner) throws SampleCleanSyntaxException
    {
        ArrayList<String> queryTokens = new ArrayList<String>();

        if (queryScanner.hasNext())
            queryTokens.add(queryScanner.next().toLowerCase());
        else
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        if (queryScanner.hasNext())
            queryTokens.add(queryScanner.next().toLowerCase());
        else
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        if (queryScanner.hasNext())
            queryTokens.add(queryScanner.next().toLowerCase());
        else
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        if (queryScanner.hasNext())
            queryTokens.add(queryScanner.next());
        else
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        return queryTokens;
    }

    private ArrayList<String> getUDFRewrite(Scanner queryScanner) throws SampleCleanSyntaxException
    {
        ArrayList<String> udfComponents = new ArrayList<String>();

        if(! queryScanner.hasNext())
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        String asToken = queryScanner.next().toLowerCase();

        if(! queryScanner.hasNext())
            throw new SampleCleanSyntaxException("SampleClean: Illegal Syntax!");

        String select = queryScanner.next().toLowerCase();

        String remainingTokens = "";
        while(queryScanner.hasNext())
        {
            String currentToken = queryScanner.next().toLowerCase();

            if (currentToken.equals("from"))
                break;

            remainingTokens += currentToken + " ";
        }


        String udfString = HIVEQL_SAMPLECLEAN_UDF + "(";
        String schemaString =HIVEQL_HASH_COL_NAME+","+HIVEQL_DUP_COL_NAME;
        String [] columns = remainingTokens.split(",");

        for (int i=0;i<columns.length;i++)
        {
            String [] column = columns[i].trim().split("\\s+");
            udfString += ((i == 0)?"":",") + column[0];
            if (column.length == 2)
                schemaString += ","+column[1];
            else
                schemaString += ","+column[0];
        }
        udfString += ")";
        String exportClause = "LATERAL VIEW " + udfString + " t AS " + schemaString;

        String source = "";
        String nextClause = "";
        while(queryScanner.hasNext())
        {
            String currentToken = queryScanner.next().toLowerCase();

            if (currentToken.equals("where") || currentToken.equals("samplewith") || currentToken.equals("group"))
            {
                nextClause = currentToken;
                break;
            }

            source += currentToken + " ";
        }

        udfComponents.add(source + " " + exportClause + " " + nextClause + " ");
        udfComponents.add(schemaString);
        return udfComponents;
    }

    /*
     * This is an inner class for SampleClean specific syntax
     * errors, pretty uninteresting for now
     */
    private class SampleCleanSyntaxException extends Exception{

        public SampleCleanSyntaxException(String exception){
            super(exception);
        }

    }

}

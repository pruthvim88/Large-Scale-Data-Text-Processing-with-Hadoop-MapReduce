Copying the input text file to hdfs input directory
-------------------------------------------------------
hdfs dfs -mkdir -p ~/input/
hdfs dfs -put /media/sf_Lab4/Problem3/Tess_Files ~/input

Location details for 2 word co-occurences:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main LatinTextWordCoocPairs.java
jar cf lwcooc2pairs.jar LatinTextWordCoocPairs*.class
hadoop jar lwcooc2pairs.jar LatinTextWordCoocPairs ~/input/Tess_Files ~/LWCooC2POutput
hdfs dfs -get ~/LWCooC2POutput /media/sf_Lab4/Problem4/


Location details for 3 word co-occurences:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main LatinTextWordTriPairs.java
jar cf lwcooc3pairs.jar LatinTextWordTriPairs*.class
hadoop jar lwcooc3pairs.jar LatinTextWordTriPairs ~/input/Tess_Files ~/LWCooC3POutput
hdfs dfs -get ~/LWCooC3POutput /media/sf_Lab4/Problem4/
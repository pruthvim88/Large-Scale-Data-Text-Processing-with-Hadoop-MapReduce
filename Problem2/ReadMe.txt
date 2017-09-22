Copying the input text file to hdfs input directory
-------------------------------------------------------
hdfs dfs -mkdir -p ~/input/
hdfs dfs -put Cannes_TweetText ~/input


Word co-occurence using Pairs:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main PairsCooccurences.java
jar cf pco.jar PairsCooccurences*.class
hadoop jar pco.jar PairsCooccurences ~/input/Cannes_TweetText ~/PairsOutput
hdfs dfs -get ~/PairsOutput /media/sf_Lab4/Problem2/


Word co-occurence using Stripes:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main StripesCooccurences.java
jar cf sco.jar StripesCooccurences*.class
hadoop jar sco.jar StripesCooccurences ~/input/Cannes_TweetText ~/StripesOutput
hdfs dfs -get ~/StripesOutput /media/sf_Lab4/Problem2/

References:
http://stackoverflow.com/questions/23209174/converting-mapwritable-to-a-string-in-hadoop
http://www.javased.com/index.php?source_dir=Cloud9/src/dist/edu/umd/cloud9/example/cooccur/ComputeCooccurrenceMatrixPairs.java

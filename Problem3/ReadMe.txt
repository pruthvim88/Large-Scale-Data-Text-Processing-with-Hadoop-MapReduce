Copying the input text file to hdfs input directory
-------------------------------------------------------
hdfs dfs -mkdir -p ~/input/
hdfs dfs -put /media/sf_Lab4/Problem3/Tess_Files ~/input

Location details of each latin word with their lemmas:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main LatinWordCount.java
jar cf lwc.jar LatinWordCount*.class
hadoop jar lwc.jar LatinWordCount ~/input/Tess_Files ~/LCOutput
hdfs dfs -get ~/LCOutput /media/sf_Lab4/Problem3/

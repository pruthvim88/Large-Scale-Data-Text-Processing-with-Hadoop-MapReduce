Copying the input text file to hdfs input directory
-------------------------------------------------------
hdfs dfs -mkdir -p ~/input/
hdfs dfs -put Hashtags_Cannes ~/input

Word Count in a text file:
-------------------------------------------------------
hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount ~/input/Hashtags_Cannes ~/WCOutput
hdfs dfs -get ~/WCOutput /media/sf_Lab4/Problem2/

Steps for running:
1) The Stripes.java is the java program for finding the pairs. 
2) The following commands are run to load into the hdfs
	hadoop com.sun.tools.javac.Main Lemmatizer.java
	jar cf Lemmatizer.jar Lemmatizer*.class
	hadoop jar Lemmatizer.jar Lemmatizer ~/input/Q3 ~/output1
	hdfs dfs -cat ~/output1/*
3) The new_lemmatizer.csv is saved the below location ~/Question3/Lemmatizer_JavaFiles/lemma
3) The output from the hadoop is saved into the InputOutput folder. 

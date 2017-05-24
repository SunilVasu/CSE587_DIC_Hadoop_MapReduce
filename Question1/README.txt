Steps for running:
1) The part1.R is the started application which collects the tweets and save as csv file.
The first half of the part1.R is used to generate the tweets. The second half is used to
generate the word clouds. Collected tweets are saved as a csv file test1.csv.
2) The CSV file test1.csv is loaded into the word count program. The program is loaded into
the Hadoop DFS and with the input test1.csv we run the word count program.
3) The output is obtained as saved as a CSV file for processing in the R program.
4) The second half of the part1.R will process the output obtained from the HDFS. The word cloud is
generated.

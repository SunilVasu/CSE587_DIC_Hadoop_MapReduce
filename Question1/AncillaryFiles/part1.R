library(twitteR)

api_key <-  "tiW5p98kts1D3zOi4KilX17aM"
api_secret <- "7cfHHNwdwJiWptxgGbbSmMjAyTNpcdcMIZsP5bGjunavk6DJl2"
access_token <- "1145322679-uippfeROvQQR4ioaZnSC3LsVMIkCZfWBVJARq5d"
access_secret <- "zNVh9Yo5EcoCogI27NRvjD2qOLcOFOgXywjCNb2WN2Oin"

setup_twitter_oauth(api_key, api_secret, access_token, access_secret)
key <- 'Science'

#Search only in US
tweets <- searchTwitter(key, n=250, lang="en",geocode="39.8,-95.583068847656,2500km")

#total number of tweet
cat ("\nTotal tweet:\t", length(tweets));

#Convert to DF
data_df <- twListToDF(tweets)
cat ("\nTotal data_df:\t",nrow(data_df));
#data_df
df <- data.frame(data_df$text)

#data_df - The collected tweets are saved as csv file which is processed to find the word count.
write.csv(df, '/home/sunil/WorkSpace/R/DIC_Lab4/Lab4/Question1/InputOutput/in/test1.csv')
#save(data,file="/home/sunil/WorkSpace/R/DIC_Lab4/Question1/p1_data.R")
#data
############## After Map Reduce is called on the Collected tweets ###############
#Read csv file
mydata <- read.csv("/home/sunil/WorkSpace/R/DIC_Lab4/Lab4/Question1/InputOutput/output2/part-r-00000.csv")
mydata <- mydata[, 1:2]
mydata
mydata <- data.frame(mydata)
mydata
#grep("^#", mydata$X.., value = TRUE)
#mydata[!grepl("^#", mydata$X..),]
df<-mydata[grepl("^#|^@", mydata$X..) , ]   
df
hash_df <- mydata[grepl("^#", mydata$X..) , ]  
at_df <- mydata[grepl("^@", mydata$X..) , ]  

#Data cleaning
hash_df <- hash_df[!is.na(as.numeric(as.character(hash_df$X1))),]
hash_df
at_df <- at_df[!is.na(as.numeric(as.character(at_df$X1))),]
at_df

cat ("\n hash_df row:\t",nrow(hash_df)," col:\t",ncol(hash_df))
cat ("\n at_df row:\t",nrow(at_df)," col:\t",ncol(at_df))

colnames(hash_df)

#install.packages("wordcloud")
#install.packages("slam")
#install.packages("tm")
library(wordcloud)
library(tm)

#install.packages("tm")  # for text mining
#install.packages("SnowballC") # for text stemming
#install.packages("wordcloud") # word-cloud generator 
#install.packages("RColorBrewer") # color palettes
# Load
#library("slam")
library("SnowballC")
library("wordcloud")
library("RColorBrewer")
#detach("slam")

#library(devtools)
#install_url("https://cran.r-project.org/src/contrib/Archive/slam/slam_0.1-37.tar.gz")
#library(dplyr)

#wordcloud for hast tag
wordcloud(words = hash_df$X.., freq = as.numeric(as.character(hash_df$X1)), min.freq = 1)
#wordcloud for @ tags
wordcloud(words = at_df$X.., freq = as.numeric(as.character(at_df$X1)), min.freq = 1)

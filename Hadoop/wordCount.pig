-- Loading input file from HDFS
inputFile = LOAD 'hdfs:///user/DeepaP/pigInput.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combining the words from the above stage
grpd = GROUP words BY word;
-- Counting the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group, COUNT(words);
-- Storing the result in HDFS
STORE cntd INTO 'hdfs:///user/DeepaP/file_results';
-- Loading the CSV file
salesTable = LOAD 'hdfs:///user/DeepaP/sales.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Grouping data using the country column
GroupByCountry = GROUP salesTable BY Country;
-- Generating the result format
CountByCountry = FOREACH GroupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
-- Saving the result in HDFS folder
STORE CountByCountry INTO 'hdfs:///user/DeepaP/salesOutput' USING PigStorage('\t');
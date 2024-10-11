-- Load the CSV file
salesTable = LOAD 'hdfs:///user/vinay/sales1.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Group data using the country column
GroupByCountry = GROUP salesTable BY Country;
-- Generate result format
outputString = FOREACH GroupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
-- Delete the output folder
rmf hdfs:///user/vinay/PigResult3;
-- Store the result in HDFS
STORE cntd INTO 'hdfs:///user/vinay/PigResult3';

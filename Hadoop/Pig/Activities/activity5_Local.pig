-- Load the CSV file
salesTable = LOAD '/root/sales.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
-- Group data using the country column
GroupByCountry = GROUP salesTable BY Country;
-- Generate result format
outputString = FOREACH GroupByCountry GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
-- Delete the output folder
rmf /root/PigResult2;
-- Save result in local FS (File System)
STORE outputString INTO '/root/PigResult2';

-- Load input file from HDFS
inputFile = LOAD '/root/input1.txt' AS (line);
-- Tokeize each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) AS wordCount;
-- Delete the output folder
rmf /root/PigResult4;
-- Save result in local FS (File System)
STORE cntd INTO '/root/PigResult4';

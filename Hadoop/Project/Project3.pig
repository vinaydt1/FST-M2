-- Load input file from HDFS
input1 = LOAD 'hdfs:///user/vinay/Inputs/episodeIV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);
input2 = LOAD 'hdfs:///user/vinay/Inputs/episodeV_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);
input3 = LOAD 'hdfs:///user/vinay/Inputs/episodeVI_dialogues.txt' USING PigStorage('\t') AS (Name:chararray,Dialogue:chararray);

lines =UNION input1,input2,input3;

-- Combine(Group) by characther names
groupedline = GROUP lines BY Name;

-- Count the occurence of each name
cntd = FOREACH groupedline GENERATE $0 AS character, COUNT($1) AS lineCount;

-- Order the result in Descending order
Order_linecount = ORDER cntd BY lineCount DESC;

--Delete the output folder
rmf hdfs:///user/vinay/pigproject;


-- Store the result in HDFS
STORE Order_linecount INTO 'hdfs:///user/vinay/pigproject' USING PigStorage('\t');

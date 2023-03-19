CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorReadingTime` bigint COMMENT 'from deserializer',
  `serialNumber` string COMMENT 'from deserializer',
  `distanceFromObject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'case.insensitive'='TRUE', 
  'dots.in.keys'='FALSE', 
  'ignore.malformed.json'='FALSE', 
  'mapping'='TRUE') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/step_trainer/landing'
TBLPROPERTIES (
  'classification'='json', 
  'transient_lastDdlTime'='1679009998')
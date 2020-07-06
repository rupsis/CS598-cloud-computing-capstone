# Group 3


## Question 1
```
# Creating external DB in dynamoDB:

# Create local table:

CREATE TABLE cs598_question_3_1_local
    (origin STRING,
    flight_total BIGINT);


CREATE EXTERNAL TABLE ddb_cs598_question_3_1_s3_export(origin string, flight_total bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LOCATION 's3://cs598-query-results/query-results/';

INSERT OVERWRITE TABLE ddb_cs598_question_3_1_s3_export SELECT * 
FROM cs598_question_3_1_local; 

#insert into dynamodb

# To find popularity of air ports:

Aggrecate airport 

INSERT OVERWRITE TABLE cs598_question_3_1_local
select Origin, origin_total + dest_total as total from ((select Origin, count(Origin) as origin_total from `cs598.cs598task1` Group by Origin) as a 
JOIN 
(select Dest, count(Dest) as dest_total from 
`cs598.cs598task1` Group by Dest) as b
ON a.Origin = b.Dest) order by total desc;



```
Results:
```



```

Sample solution:
```


```
# Task 1 
## Get the EBS data

1. Create a EC2 instance and attach the snapshot of the data to it. (Using Ubuntu as OS was the easiest imo)
2. [mount volume](https://confluence.atlassian.com/hipchatkb/how-to-access-to-the-files-inside-of-an-elastic-block-store-volume-827111576.html)
3. navigate to the data, and zip it up: `zip -r data.zip .`
4. Download data set to local machine: `scp -i 'key.pem'  user@ec2-xx-xx-xxx-xxx.compute-1.amazonaws.com:/path/to/file ./local/path`



## Cleaning the data:
To test if a column is null: 
```
#grab a random subset of the data to observe. Ensure the header information is preserved
cat 1988.csv | head -n 1 > 1988_1_sample.csv
shuf -n 50000 On_Time_On_Time_Performance_1988_1.csv >> 1988_1_sample.csv

# grabs all of the lines minus the first, and sticks it into a file
awk -v ORS="" -F "\"*,\"*" 'NR>1  {print $10}' fike.csv

# checks a file to see if it contains all white space. If so, that column can be deleted
grep -q '[^[:space:]]' < test.txt && printf '%s\n' "$file contains something else than whitespace characters"
```


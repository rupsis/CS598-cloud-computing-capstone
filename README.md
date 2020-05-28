# Task 1 
## Get the EBS data

1. Create a EC2 instance and attach the snapshot of the data to it. (Using Ubuntu as OS was the easiest imo)
2. [mount volume](https://confluence.atlassian.com/hipchatkb/how-to-access-to-the-files-inside-of-an-elastic-block-store-volume-827111576.html)
3. navigate to the data, and zip it up: `zip -r data.zip .`
4. Download data set to local machine: `scp -i 'key.pem'  user@ec2-xx-xx-xxx-xxx.compute-1.amazonaws.com:/path/to/file ./local/path`

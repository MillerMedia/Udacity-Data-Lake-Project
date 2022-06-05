# Udacity Data Lake Project
 
This project is designed to take fictional data from a startup, Sparkify, and process it for analytics. The data is stored in an S3 data lake and through an ETL process, will be loaded, transformed and saved as parquet files that can then be used for further analytics.

In order to run the code, follow these steps:

• Log into an existing AWS account and navigate to the [IAM dashboard](https://us-east-1.console.aws.amazon.com/iamv2/home?region=us-east-1#).  
• Create a user and attach the AdministratorAccess policy to it.  
• Note the Access Key and Secret Access Key upon creation.  
• Open the dl.cfg contained within this project and paste the Access Key and Secret Access Key into this file.  
• Navigate to the [AWS S3 dashboard](https://s3.console.aws.amazon.com/s3/buckets?region=us-east-1).  
• Create a new bucket in the us-east-1 region and leave all default options as is.  
• Open the etl.py file and navigate to the `output_data` variable definition in the main() function. Set this variable to the name of the bucket you have created in the form: `s3a://{BUCKET_NAME}`  
• Open a terminal window and navigate to the directory containing these project files.  
• Install 'pyspark' using the command `pip install pyspark`
• Run the etl.py file using the command `python etl.py`  
• Once the script has completed, open the S3 bucket to confirm that files have been saved.

## Summary of files
**dl.cfg** - Configuration file containing AWS credentials  
**etl.py** - ETL file that loads, process and re-saves data into parquest files
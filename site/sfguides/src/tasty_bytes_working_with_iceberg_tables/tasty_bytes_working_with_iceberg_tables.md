author: nadithya-sf
id: tasty_bytes_working_with_iceberg_tables
summary: This tutorial is a guide built as an extention to the Tasty Bytes series, where we will go over how data from an external Data Lake is made available in Snowflake using External Tables & Iceberg Tables.
categories: Getting-Started
environments: web
status: Published 
feedback link: https://github.com/Snowflake-Labs/sfguides/issues
tags: Getting Started, Data Engineering, Iceberg Tables, AWS, S3 

# Working with Iceberg Tables
<!-- ------------------------ -->
## Overview 
Duration: 2

![banner](assets/banner.png)

Welcome to the Powered by Tasty Bytes - Working with Iceberg Tables Quickstart.

Within this Quickstart we will walk through how we can use Iceberg Tables in combination with External tables to manage data sitting in an external storage.

### Background

The Tasty Bytes team recognizes the critical importance of analyzing restaurant reviews for understanding customer satisfaction, enhancing service quality, and upholding a positive reputation. By leveraging insights from reviews, they aim to pinpoint strengths, weaknesses, and areas for improvement, thereby driving business success and gain customer loyalty.

The Data Enginerring team has been tasked to make this data available to the Data Science team. To achieve these objectives, the team is establishing an environment where development teams can access and process data from their data lake concurrently, using a variety of tools. They prioritize several key factors:

- ACID compliance
- Single source of truth
- Faster adoption
- Performance & Scalability
- Easy to maintain

![banner](assets/architecture_diagram.png)

### Prerequisites

- Snowflake account.
- AWS account with permission to create and manage IAM policies and roles.
- An S3 bucket, in the same region as your Snowflake account.

### What You’ll Learn 
- How to create an External Table
- How to create a Snowflake-managed Iceberg Table 
- Manage a Iceberg Table

### Architecture


We have an external tables that query data stored in the external storage. The External Table will create certain file-level metadata, version identifiers and related properties. The external tables are configured to auto-refresh. An iceberg table is built using the data in the external table with Snowflake as the catalog. A scheduled task keeps the iceberg table up-to-date with the latest data.

<!-- ------------------------ -->
## Setup
Duration: 15

Create a database, schema and warehouse in your Snowflake account

```sql

--Database
CREATE OR REPLACE DATABASE TASTY_BYTES_DB;

--Schema
CREATE OR REPLACE DATABASE RAW;

--Warehouse
CREATE OR REPLACE WAREHOUSE iceberg_quickstart_wh with
WAREHOUSE_SIZE = MEDIUM
AUTO_SUSPEND = 60
```

### Load data to S3

- Download the zip file from [here](https://drive.google.com/drive/folders/1w5qm8qECpOOxDqrSR7y_gHhzWZfCgFuI?usp=sharing)
- If you are using a Mac, **'.DS_Store'** files are auto generated into the folder you unzip. You need to delete these files before you upload to S3
    - Navigate to the folder **truck_reviews**.
    - Open up terminal from this folder.
    - Type **'find . -name '.DS_Store' -type f -delete'** in the terminal. 
    - Press enter.
- Open S3.
- **IMPORTANT: Iceberg tables currently need you to create your S3 bucket in the same region as your snowflake account. Make sure your snowflake account's region and S3 bucket's region align.**
    - You can know your snowflake account's region by running the below command.
    ```
    SELECT current_region();
    ```
- Open the bucket you intend to use and upload the **truck_reviews** folder.

### Create IAM Policy

- Log into the AWS Management Console.
- From the home dashboard, choose Identity & Access Management (IAM)
- Choose Account settings from the left-hand navigation pane.
- Choose Policies from the left-hand navigation pane.
- Click Create Policy.
- Click the JSON tab.
- Paste the following JSON. This will provide snowflake with **READ WRITE ACCESS** to the bucket you created.
   - **Replace < bucket > in the below JSON with the bucket name you created.**

````
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject",
              "s3:GetObjectVersion"
            ],
            "Resource": "arn:aws:s3:::<bucket>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "*"
                    ]
                }
            }
        }
    ]
}
````

### Create IAM Roles

- Log into the AWS Management Console.
- From the home dashboard, choose Identity & Access Management (IAM)
- Choose Account settings from the left-hand navigation pane.
- Choose Roles from the left-hand navigation pane.
- Click the Create role button.
- Select Another AWS account as the trusted entity type.
- In the Account ID field, enter your own AWS account ID temporarily. Later, you will modify the trusted relationship and grant access to Snowflake.
- Select the Require external ID option. Enter a dummy ID such as 0000.
- Click the Next button.
- Locate the policy from the previous step and select it.
- Click **Next** button.
- Enter a name and description for the **storage integration**  role, and click the Create role button.
    - Role Name: **iceberg_demo_stg_int_role< first letter of your first name >< last name >**
    - Example: **iceberg_demo_stg_int_role_jdoe**
- Save the Role ARN, you can altenatively comeback to this when we use it in the future steps.
- Create another role for the **external volume** with the same policy we used earlier.
    - Role Name: **iceberg_demo_ext_vol_role< first letter of your first name >< last name >**
    - Example: **iceberg_demo_ext_vol_role_jdoe**

### Create a Storage Integration

A storage integration is a Snowflake object that stores a generated identity and access management (IAM) entity for your external cloud storage, along with an optional set of allowed or blocked storage locations (Amazon S3, Google Cloud Storage, or Microsoft Azure). Cloud provider administrators in your organization grant permissions on the storage locations to the generated entity. This option allows users to avoid supplying credentials when creating stages or when loading or unloading data.

```
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE STORAGE INTEGRATION int_tastybytes_truckreviews
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '< iam_role >'
  STORAGE_ALLOWED_LOCATIONS = ('s3://< bucket >/truck_reviews/');
```

### Create External Volume

- Run the below command on your snowflake worksheet.
    - Make sure you replace **< bucket >** with the bucket name you created earlier.
    - Make sure you replace **< iam_role >** with the ROLE ARN of the **external volume** role you created earlier
        - Example ROLE ARN: **arn:aws:iam::001234567890:role/myrole**

````
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE EXTERNAL VOLUME vol_tastybytes_truckreviews
    STORAGE_LOCATIONS =
        (
            (
                NAME = 'us-west-2'
                STORAGE_PROVIDER = 'S3'
                STORAGE_BASE_URL = 's3://< bucket >/'
                STORAGE_AWS_ROLE_ARN = '< iam_role >'
            )
        );
````
### Step 6 - Retrieve the AWS IAM User for your Snowflake Account

**Policy document for IAM role**

````
  {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
````

#### Storage Integration

A storage integration is a Snowflake object that stores a generated identity and access management (IAM) user for your S3 cloud storage, along with an optional set of allowed or blocked storage locations (i.e. buckets). Cloud provider administrators in your organization grant permissions on the storage locations to the generated user. This option allows users to avoid supplying credentials when creating stages or loading data.

- Execute the DESCRIBE INTEGRATION command to retrieve the ARN for the AWS IAM user that was automatically created for your Snowflake Account.

````
DESC INTEGRATION int_tastybytes_truckreviews;
````

  - The STORAGE_AWS_IAM_USER_ARN is the snowflake user arn that has been auto generated.
  - The STORAGE_AWS_EXTERNAL_ID is the External ID you need to provide in AWS Role you created earlier.
  - Go back to the AWS management console.
  - Go back to IAM and open the **storage integration** role you created.
  - Click on the Trust relationships tab.
  - Click the Edit trust relationship button.
  - Modify the policy document with the DESC STORAGE INTEGRATION results. 
    - Update the **< STORAGE_AWS_IAM_USER_ARN >** and **< STORAGE_AWS_EXTERNAL_ID >** and click on Update Trust Policy.


#### External Volume

- Execute the DESCRIBE EXTERNAL VOLUME command to retrieve the ARN for the AWS IAM user that was automatically created for your Snowflake Account.

````
DESC EXTERNAL VOLUME vol_tastybytes_truckreviews;
````
  **Example:**

![External Volume](assets/external_volume.gif)

  - Click on the property value for the property **STORAGE_LOCATION_1**.  
  - The STORAGE_AWS_IAM_USER_ARN is the snowflake user arn that has been auto generated.
  - The STORAGE_AWS_EXTERNAL_ID is the External ID you need to provide in AWS Role you created earlier.
  - Go back to IAM and open the **external volume** role you created.
  - Click on the Trust relationships tab.
  - Click the Edit trust relationship button.
  - Modify the policy document with the DESC EXTERNAL VOLUME results. 
    - Update the **< STORAGE_AWS_IAM_USER_ARN >** and **< STORAGE_AWS_EXTERNAL_ID >** and click on Update Trust Policy.

---

You can see the source metadata for this guide you are reading now, on [the github repo](https://raw.githubusercontent.com/Snowflake-Labs/sfguides/master/site/sfguides/sample.md).


<!-- ------------------------ -->
## Create an External Table
Duration: 5

An external table is a Snowflake feature that allows you to query data stored in an external stage as if the data were inside a table in Snowflake. The external stage is not part of Snowflake, so Snowflake does not store or manage the stage. External tables let you store (within Snowflake) certain file-level metadata, including filenames, version identifiers, and related properties.

### External Stage

The review files in our external storage are in csv format and have **|** as the delimiter. So let's create a file format for this configuration.

````
CREATE OR REPLACE FILE FORMAT raw.ff_csv
    TYPE = 'csv'
    SKIP_HEADER = 1   
    FIELD_DELIMITER = '|';
````

Next let's create a stage named stg_truck_reviews in the RAW schema we created earlier for this demo. The cloud storage URL includes the path files. The stage references a storage integration named my_storage_int.

**Replace the <bucket_name> in the below code with the bucket you are using for this quickstart**

````
CREATE OR REPLACE STAGE raw.stg_truck_reviews
    STORAGE_INTEGRATION = int_tastybytes_truckreviews
    URL = 's3://<bucket_name>/truck_reviews/'
    FILE_FORMAT = raw.ff_csv;
````

#### Query the Stage

````
SELECT TOP 100 METADATA$FILENAME,
       SPLIT_PART(METADATA$FILENAME, '/', 4) as source_name,
       CONCAT(SPLIT_PART(METADATA$FILENAME, '/', 2),'/' ,SPLIT_PART(METADATA$FILENAME, '/', 3)) as quarter,
       $1 as order_id,
       $2 as truck_id,
       $3 as language,
       $4 as source,
       $5 as review,
       $6 as primary_city,
       $7 as customer_id,
       $8 as year,
       $9 as month,
       $10 as truck_brand,
FROM @raw.stg_truck_reviews/;
````
You should see a similar output as below.

![query_stage](assets/query_stage.png)

### External Table

Let's now create an external table in the RAW schema. When queried, an external table reads data from a set of one or more files in a specified external stage and outputs the data in a single VARIANT column.

Here we are parsing the semi-structured data returned in the Variant column.

````
CREATE OR REPLACE EXTERNAL TABLE raw.ext_survey_data
(
    source varchar as SPLIT_PART(METADATA$FILENAME, '/', 4),
    quarter varchar as CONCAT(SPLIT_PART(METADATA$FILENAME, '/', 2),'/' ,SPLIT_PART(METADATA$FILENAME, '/', 3)),
    order_id variant as IFNULL((value:c1),-1),
    truck_id bigint as (IFNULL(value:c2::int,-1)),
    language varchar as (value:c3::varchar),
    review varchar as (value:c5::varchar),
    primary_city varchar as (value:c6::varchar),
    review_year int as (value:c8::int),
    review_month int as (value:c9::int)
)
PARTITION BY (quarter, source)
LOCATION = @raw.stg_truck_reviews/
AUTO_REFRESH = TRUE
FILE_FORMAT = raw.ff_csv
PATTERN ='.*truck_reviews.*[.]csv';
````

## Configure External Table to Auto-Refresh

Execute the SHOW EXTERNAL TABLES command.

````
SHOW EXTERNAL TABLES;
````

Copy the ARN of the SQS queue for the external table in the **notification_channel** column and follow the below steps.

- Log into the AWS Management Console.
- Configure an event notification for your S3 bucket using the instructions provided in the Amazon S3 documentation. Complete the fields as follows:

    - **Name**: Name of the event notification (e.g. Auto-ingest Snowflake).

    - **Events**: Select the ObjectCreate (All) and ObjectRemoved options.

    - **Send to**: Select SQS Queue from the dropdown list.

    - **SQS**: Select Add SQS queue ARN from the dropdown list.
    
    - **SQS queue ARN**: Paste the SQS queue name from the SHOW EXTERNAL TABLES output.

The external stage with auto-refresh is now configured!

When new or updated data files are added to the S3 bucket, the event notification informs Snowflake to scan them into the external table metadata.



<!-- ------------------------ -->
## Create Iceberg Table
Duration: 5

An Iceberg table uses the Apache Iceberg open table format specification, which provides an abstraction layer on data files stored in open formats and supports features such as:

- ACID (atomicity, consistency, isolation, durability) transactions
- Schema evolution
- Hidden partitioning
- Table snapshots

Iceberg tables for Snowflake combine the performance and query semantics of regular Snowflake tables with external cloud storage that you manage. They are ideal for existing data lakes that you cannot, or choose not to, store in Snowflake.

![iceberg types](assets/iceberg_table_types.png)

In this quickstart we will see how to create a Iceberg table using Snowflake as the catalog. An Iceberg table that uses Snowflake as the Iceberg catalog provides full Snowflake platform support with read and write access. The table data and metadata are stored in external cloud storage, which Snowflake accesses using an external volume. Snowflake handles all life-cycle maintenance, such as compaction, for the table.

**Note**: Check the name of the external volume and replace the BASE_LOCATION with the folder strucutre of your choice. The data and metadata files for the Iceberg table will be created in the BASE_LOCATION provided

````
CREATE OR REPLACE ICEBERG TABLE raw.icb_truck_reviews
        (
        source_name VARCHAR,
        quarter VARCHAR,
        order_id BIGINT,
        truck_id INT,
        review VARCHAR,
        language VARCHAR
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = 'vol_tastybytes_truckreviews'
        BASE_LOCATION = 'iceberg_tables/antesting/';
````

Snowflake creates data and metadata files and manages them. The table state is maintained in the metadata files. When changes are made to the data, new metadata files are created replacing the older metadata.

![iceberg_files](assets/iceberg_files_1.png)

#### Metadata Files

  - Metadata File: Stores table schema, partition info, and snapshot details.

  - Manifest List File: Indexes manifest files, tracks added/deleted data files, and includes partition boundaries.

  - Manifest File: Lists data files (Parquet/ORC/AVRO) and may include column-level metrics for optimization.

![iceberg_metadata](assets/iceberg_metadata.png)

#### Data Files

Data files refer to the physical files that store the actual data. These files typically contain data in formats such as Parquet or ORC. Iceberg tables organize data into multiple data files, each containing a subset of the table's data. These files are managed and optimized by the Iceberg table format to support efficient data storage, retrieval, and processing.



<!-- ------------------------ -->
## Managing Iceberg Table
Duration: 3

### Create a Stream

The stream will capture all the changes happening on the external table we configured earlier.

````
CREATE OR REPLACE STREAM raw.ext_reviews_stream ON EXTERNAL TABLE raw.ext_survey_data INSERT_ONLY = TRUE;
````

### Create a Task

The below task is configured to run every 6 hours and will insert any new reviews that arrive into the S3 bucket into the iceberg table.

````
CREATE OR REPLACE TASK raw.update_truck_reviews_task
WAREHOUSE = iceberg_quickstart_wh
SCHEDULE = '6 HOURS'
WHEN SYSTEM$STREAM_HAS_DATA('raw.ext_reviews_stream')
AS
BEGIN
    INSERT INTO raw.icb_truck_reviews (source_name,
        quarter,
        order_id,
        truck_id,
        review,
        language)
    SELECT source_name,
        quarter,
        order_id,
        truck_id,
        review,
        language
    FROM raw.ext_reviews_stream;
END;


ALTER TASK raw.update_truck_reviews_task RESUME;
````


## Conclusion and Next Steps

In this quickstart, we explored the use of Iceberg tables in Snowflake to manage and analyze restaurant review data efficiently. We began by setting up the necessary infrastructure in Snowflake, AWS, and S3, including creating databases, warehouses, IAM policies, roles, and storage integrations.

We then proceeded to surface data from S3 into Snowflake using external stages and tables, allowing us to query and analyze the data as if it were stored natively within Snowflake. We configured auto-refresh for the external tables to ensure they stay up-to-date with changes in the S3 bucket.

Next, we created Iceberg tables in Snowflake, leveraging the powerful features of the Iceberg format such as ACID transactions, schema evolution, and hidden partitioning. These tables allow us to efficiently manage and analyze large volumes of data stored in external cloud storage while benefiting from Snowflake's query capabilities.

To keep our Iceberg table up-to-date with the latest data, we set up a scheduled task that runs every 6 hours and inserts new reviews from an external stream into the Iceberg table. This ensures that our analysis is always based on the most recent data available.

### Related Resources

  - [Snowflake Documentation for Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
  - [Apache Iceberg Documentation](https://iceberg.apache.org/)


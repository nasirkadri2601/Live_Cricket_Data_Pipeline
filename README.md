# Live_Cricket_Data_Pipeline

This project is designed to capture live cricket data in raw JSON format from multiple data sources. The raw data is then transformed and cleaned to ensure consistency and accuracy. After the transformation process, the final data is stored in a centralized data warehouse for further analysis. The analysis could include a wide range of use cases such as predicting match outcomes, player performance analysis, and insights into team strategies, among other advanced analytics. The entire pipeline ensures that high-quality, real-time cricket data is available for stakeholders to derive meaningful insights and make data-driven decisions.
![Live_Cricket_Data_Pipeline](https://github.com/user-attachments/assets/1e0ce7ce-e6a0-4a92-a656-de78400820c1)

Below are the step by step process to execute this project.


**Step_1)** This project uses two data sources: an API and web scraping. First, create an API by signing up at Cricket Live Line API: https://rapidapi.com/apiservicesprovider/api/cricket-live-line1, where you'll receive an API key for accessing the data. For web scraping, we will extract data from ESPN Cricinfo: https://www.espncricinfo.com/.
 
**Step_2)** Using Python, we will write a script deployed in AWS Lambda to fetch and merge data from both sources into a single raw JSON format. 

**Step_3)** Create an S3 bucket with two main folders: raw_data and transformed_data. Inside the raw_data folder, create two subfolders: processed and to_processed. Data from the to_processed folder will be moved to processed after being utilized, preventing duplication during processing. In the transformed_data folder, create seven subfolders for different datasets that will be stored as CSV files and later loaded into 7 distinct tables in the data warehouse. These folders are:

Summary_Data

Status_Data

Commentary_Data

Team_A_Data

Team_B_Data

Batsmen_Data

Bowler_Data


**Step_4)** Create a Lambda function in AWS and begin writing the code provided in the repository named Extraction_AWS_Lambda_Code. Carefully review the code and make necessary changes, including inserting your API key and the correct ESPN Cricinfo URL. Once the code is updated and executed successfully, the raw data (in JSON format) will be stored in the raw_data/to_processed folder on S3.

Note: Additionally, create two Lambda layers and upload the zip file I’ve attached. These layers are required to import the bs4 and requests libraries.
Finally, navigate to the Lambda function configuration settings, then go to Permissions. You will see the default role listed there. Click on it and attach the appropriate permissions, such as S3 Full Access, to enable communication between Lambda and S3 

**Step_5)** First, create a Databricks account. Once you have successfully created the account, begin writing the transformation logic using the code provided in the repository named Transformation_Databricks_Code. Carefully review the code and make necessary modifications, such as replacing the AWS access and secret keys with your own credentials.
At the end of this process, all the transformed data will be stored in the appropriate folders under transformed_data/{respective folders}. Additionally, the data will be moved from the to_processed folder to the processed folder to avoid duplication and ensure proper data flow.

**Step_6)** To orchestrate the entire process, we will use Apache Airflow. First, install Airflow with the help of Docker. You can follow any tutorial or YouTube video to guide you through the installation process. Once Docker is set up, use the YAML configuration file that I have attached in the repository to configure your Airflow environment.
With the YAML file in place, run the Docker container. This will initiate Airflow, which you can access through localhost:8080 in your web browser. From this point, you will be able to monitor and manage the execution of the entire ETL process directly from the Airflow UI.

**Step_7)** Start by writing the Airflow code that will run every 55 seconds, triggering two functions: the Extraction Lambda function and the Transformation Databricks code. After logging into Airflow, configure the connections between Airflow and AWS, as well as Airflow and Databricks. To do this, navigate to the Connections tab in Airflow, click the '+' sign, and enter the required details for both AWS and Databricks.
 
**Step_8)** Once the transformed data is generated, we need to load it into the Snowflake data warehouse. First, create a Snowflake account and write the necessary SQL code. Begin by creating a storage integration for the AWS S3 bucket, which will store the data in an external stage. This external stage will allow us to view all the data files present in the S3 folder. After that, create the seven required tables and then set up seven different Snowflake pipes. These pipes will automatically trigger and load the data into their respective tables as soon as a new file is uploaded to the S3 transformed data folder.

Note: Creating the Pipeline Connection from AWS to Snowflake: When creating the storage integration, the STORAGE_AWS_ROLE_ARN value should be retrieved from AWS IAM roles. Create a new IAM role and grant it full S3 access (navigate to IAM → Roles → select the role, then copy the ARN value).
Additionally, under the Describe Integration section in Snowflake, you will find two values: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID. These values must be added to the AWS role's trust relationship. Navigate to the specific role in AWS IAM, go to Trust Relationship, and replace the existing AWS and External Id values with the corresponding values from Snowflake.
After creating the pipes in Snowflake, run a DESCRIBE PIPE command. This will generate a notification channel URL. Copy this URL and set up seven different event notifications in the S3 bucket properties to monitor each of the transformed data folders

**Step_9)** After that connect with Power Bi and start analysing the data accordingly.

If you need help ping me on linked: https://www.linkedin.com/in/nasir-kadri-data-engineer 


 

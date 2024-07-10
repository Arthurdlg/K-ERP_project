# F-5-project

<<<<<<< HEAD
# Build project
"Run Docker"
docker-compose up -build

"Install dependancies"
pip install kafka-python

"Build topics if not exist"
python ./src/build_topics.py

# Run
python ./src/producer_kafka.py
python ./src/consumer_spark.py
=======

# Project 1:
Description:
1. Build a system for K-ERP solutions to handle real-time transactions
for an e-commerce platform.
2. The data will be consumed for various operations such as inventory
management, order processing, and user notifications. So , cleaning of
the data is an important part before storing it in database.
3. The processed data should be stored in a relational database for transaction integrity.


# Project Report
- Introduction about the Project.
- Tools Used (ex Spark , kafka , ...)
- Explanation about how you achieve your target or you didn't acheive it (Each student can explain his own work) For e.g Student A has worked on kafka for live streaming ,so he can present his part . Student 2 if have worked on Spark , will present the transformation of the dataset. Student 3 , if used database then how the schema is prepared and how the data was stored in how many tables , schema etc. If Student 4 is their , then he can explain the overall working or the demo of the project.
- The report for submission needs to be in the PPT format
>>>>>>> e3d047fda5f4ed744a045a076ae8c7cf332aa560

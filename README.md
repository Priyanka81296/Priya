Youtube-Data-Harvesting-and-Warehousing
This project uses the YouTube API to extract data from YouTube channels, playlists, and videos.

Overview
This project aims to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels

Prerequisites
Python programming language
googleapiclient library
pymongo library
mysql-connector-python library
streamlit library
YouTube Data API key (Get one from the Google Developers Console [https://console.developers.google.com/])
MongoDb Connection
Install MongoDb and MongoDb Compass
mongodb://localhost:27017
MySQL Connection
Install MySQL Server and MySQL Workbench
Create a local connection and a database
Steps involved
Create API key and database connections
Extract data from youtube api using google api key and store it in MongoDb
Migration data from MongoDb to a SQL database for efficient querying and analysis
Search and retrieve data from SQL database using different search options Note: Assumption is youtube_data_harvesting database is already created in MySQL

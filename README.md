Youtube-Data-Harvesting-and-Warehousing
This project uses the YouTube API to extract data from YouTube channels and videos.
Overview
This project aims to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels

Technology Stack Used

Python
Google Client Library
MySQL
MongoDB
Streamlit
Steps involved

Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python
2.Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 3 different collections.
3.Transferring the collected data from multiple channels namely the channels, videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL for this purpose.
4.Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the the foreign and the primary key.
5.Finally these input and output are given in streamlit. outputs are shown in streamlit using streamlit code

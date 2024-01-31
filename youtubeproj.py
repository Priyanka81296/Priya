from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
import pymongo
import pymysql
import streamlit as st

api_key = 'AIzaSyAT8b5gAVZdtfaq3uEPdWIiyrcJMZkbZDM'

api_service_name = "youtube"
api_version = "v3"
developerkey=api_key

def api_connect():
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=api_connect()

#channel details
def channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    for i in response['items']:
        channel_data = dict(
        channel_name = i['snippet']['title'],
        channel_id  = i['id'],
        channel_des = i['snippet']['description'],
        channel_playlist = i['contentDetails']['relatedPlaylists']['uploads'],
        channel_viewcnt = i[ 'statistics']['viewCount'],
        channel_subcnt  = i[ 'statistics']['subscriberCount'],
        channel_videocnt =i[ 'statistics']['videoCount'])

    return channel_data

#videos info
def get_videos_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(
            part='contentDetails',
            id=channel_id)
    response = request.execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(
                                         part='snippet',
                                         playlistId = Playlist_Id,
                                         maxResults=50,
                                         pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

Video_Ids=get_videos_ids('UCvaMpohMIVypvEWBEOHo1wA')

def get_video_info(videos_ids):
    video_data=[]
    for video_id in Video_Ids:
        request = youtube.videos().list(
             part = 'snippet,contentDetails,statistics',
             id = video_id
        )
        response = request.execute()

        for item in response["items"]:
                data=dict(Channel_Name=item['snippet']['channelTitle'],
                         Channel_Id=item['snippet']['channelId'],
                         Video_Id=item['id'],
                         Title=item['snippet']['title'],
                         Tags="".join(item['snippet'].get('tags')),
                         Thumbnail=item['snippet']['thumbnails']['default']['url'],
                         Description=item['snippet'].get('description'),
                         Published_Date=item['snippet']['publishedAt'],
                         Duration=item['contentDetails']['duration'],
                         Views=item['statistics'].get('viewCount'),
                         Likes=item['statistics'].get('likeCount'),
                         comments=item['statistics'].get('commentCount'),
                         Favorite_Count=item['statistics']['favoriteCount'],
                         Definition=item['contentDetails']['definition'],
                         Caption_Status=item['contentDetails']['caption']
                         )
                video_data.append(data)
    return video_data

def get_comment_info(video_ids):
    Comment_data=[]
    try:
            for video_id in Video_Ids:
                request = youtube.commentThreads().list(
                    part = 'snippet',
                    videoId = video_id,
                    maxResults = 50
                )
                response = request.execute()

            for item in response['items']:
                    data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                               Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                               Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                               Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                               Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])

                    Comment_data.append(data)
    except:
        pass
    return Comment_data
comment_details=get_comment_info(Video_Ids)
#playlist info
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
                part = 'snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                      Title=item['snippet']['title'],
                      Channel_Id=item['snippet']['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      PublishedAt=item['snippet']['publishedAt'],
                      Video_count=item['contentDetails']['itemCount'])
            All_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

#mongodb connection

client=pymongo.MongoClient('mongodb://localhost:27017')
db=client['Youtube_part']

def chanel_data(channel_id):
    chan_details1=channel_info(channel_id)
    play_details1=get_playlist_details(channel_id)
    vi_ids1=get_videos_ids(channel_id)
    vi_details1=get_video_info(vi_ids1)
    com_details1=get_comment_info(vi_ids1)
    
    collec=db["chanel_data"]
    collec.insert_one({"channel_information":chan_details1,"playlist_information":play_details1,
                      "video_information":vi_details1,"comment_information":com_details1})
    
    return "upload completed successfully"

#sql connection
myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='root')

def channels_table():
    
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='root',database='youtube_parts',port=3306)
    cursor = myconnection.cursor()

    drop_query='''drop table if exists chanels'''
    cursor.execute(drop_query)
    myconnection.commit()

    create_query='''create table if not exists chanels(channel_name varchar(100),
                                                      channel_id varchar(80) primary key,
                                                      channel_subcnt bigint,
                                                      channel_viewcnt bigint,
                                                      channel_videocnt int,
                                                      channel_des text,
                                                      channel_playlist varchar(80))'''
    cursor.execute(create_query)
    myconnection.commit()
    
    ch_list=[]
    db=client['Youtube_part']
    collec=db["part_data"]



    for ch_data in collec.find({}, {"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)
        
    for index,row in df.iterrows():
            insert_query='''insert into chanels(channel_name,
                                                    channel_id,
                                                    channel_subcnt,
                                                    channel_viewcnt,
                                                    channel_videocnt,
                                                    channel_des,
                                                    channel_playlist)

                                                    values(%s,%s,%s,%s,%s,%s,%s)'''


            values=(row['channel_name'],
                    row['channel_id'],
                    row['channel_subcnt'],
                    row['channel_viewcnt'],
                    row['channel_videocnt'],
                    row['channel_des'],
                    row['channel_playlist'])

            cursor.execute(insert_query,values)
            myconnection.commit()

def video_table():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='root',database='youtube_parts',port=3306)
    cursor = myconnection.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    myconnection.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                            Channel_Id varchar(100),
                                                            Video_Id varchar(30),
                                                            Title varchar(150),
                                                            Tags text,
                                                            Thumbnail varchar(200),
                                                            Description text,
                                                            Published_Date varchar(100),
                                                            Duration varchar(100),
                                                            Views BIGINT,
                                                            Likes bigint,
                                                            comments int,
                                                            Favorite_Count int,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(50))'''

    cursor.execute(create_query)
    myconnection.commit()

    vi_list=[]
    db=client['Youtube_part']
    collec=db["chanel_data"]



    for vi_data in collec.find({}, {"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status) 

                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


            values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],     
                        "".join(row['Tags']),
                        row['Thumbnail'],                   
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])

            cursor.execute(insert_query,values)
            myconnection.commit()

    
def comments_table():
    myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='root',database='youtube_parts',port=3306)
    cursor = myconnection.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    myconnection.commit()

    create_query='''create table if not exists comments(Comment_Id  varchar(100),
                                                        Video_Id varchar(100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published varchar(100)
                                                        )'''
    cursor.execute(create_query)
    myconnection.commit()

    com_list=[]
    db=client['Youtue_data']
    collec=db["chanel_data"]




    for com_data in collec.find({}, {"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)

                                            values(%s,%s,%s,%s,%s)'''


        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
                )

        cursor.execute(insert_query,values)
        myconnection.commit()

def tables():
    channels_table()
    video_table()
    comments_table()
    
    return "Table Created Successfully"

def show_channels_table():
  ch_list=[]
  db=client['Youtube_part']
  collec=db["chanel_data"]



  for ch_data in collec.find({}, {"_id":0,"channel_information":1}):
      ch_list.append(ch_data["channel_information"])


  df=st.dataframe(ch_list)  

  return df

def show_videos_table():
    vi_list=[]
    db=client['Youtube_part']
    collec=db["chanel_data"]



    for vi_data in collec.find({}, {"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():

    com_list=[]
    db=client['Youtube_part']
    collec=db["chanel_data"]


    for com_data in collec.find({}, {"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

        df3=st.dataframe(com_list)

        return df3
    
with st.sidebar:
    st.title(":green[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header(":Languages and skills taken")
    st.caption("Python Language")
    st.caption("API Integration")
    st.caption("Data collection")
    st.caption("MongoDB Insersion")
    st.caption("Connection to sql")
    st.caption("Data Management using Mongodb and sql")
channel_id=st.text_input("Enter the channel ID")

if st.button("Store the data"):
    ch_ids=[]
    db=client['Youtube_part']
    collec=db["chanel_data"]
    for ch_data in collec.find({}, {"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=chanel_data(channel_id)
        st.success(insert)
    
if st.button("Transfer to sql"):
    Table=tables()
    st.success(Table)
show_table=st.radio("Select the Table",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table

myconnection = pymysql.connect(host='127.0.0.1',user='root',passwd='root',database='youtube_parts',port=3306)
cursor = myconnection.cursor()

questions=st.selectbox("Select your question",("1.All videos and their corresponding channels names",
                                              "2.Channels with most number of videos",
                                              "3.Top 10 most viewed videos and their respective channels",
                                              "4.Comments on each video, and what are theircorresponding video names",
                                              "5.Videos with the highest number of likes and their channel names",
                                              "6.Total number of likes & dislikes for each video and their video names",
                                              "7.Total number of views for each channel, and their channel names",
                                              "8.Names of all the channels that have published videos in the year 2022",
                                              "9.Average duration of all videos in each channel, and their channel names",
                                              "10.Videos with highest number of comments, and their channel names",))


if questions == '1.All videos and their corresponding channels names':
    Query1="""SELECT Title AS VideoTitle, Channel_Name AS Channelname FROM videos ORDER BY Channel_Name"""
    cursor.execute(Query1)
    myconnection.commit()
    t1=cursor.fetchall()
    df = pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif questions == '2.Channels with most number of videos':
    Query2="""SELECT channel_name AS Channelname,channel_videocnt as no_videos FROM chanels ORDER BY channel_videocnt desc"""
    cursor.execute(Query2)
    myconnection.commit()
    t2=cursor.fetchall()
    df2 = pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif questions == '3.Top 10 most viewed videos and their respective channels':
    Query3="""SELECT views as views,channel_name as channelname,title as videotitle from videos
            where views is not null order by views desc limit 10"""
    cursor.execute(Query3)
    myconnection.commit()
    t3=cursor.fetchall()
    df3 = pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif questions == '4.Comments on each video, and what are theircorresponding video names':
    Query4="""SELECT comments as no_comments,title as videotitle from videos where comments is not null"""
    cursor.execute(Query4)
    myconnection.commit()
    t4=cursor.fetchall()
    df4= pd.DataFrame(t4,columns=["no of comment","video title"])
    st.write(df4)

elif questions == '5.Videos with the highest number of likes and their channel names':
    Query5="""select Title as videotitle,Channel_Name as channelname,Likes as likecount from videos
            where Likes is not null order by Likes desc"""
    cursor.execute(Query5)
    myconnection.commit()
    t5=cursor.fetchall()
    df5= pd.DataFrame(t5,columns=["video title","channelname","likecount"])
    st.write(df5)

elif questions == '6.Total number of likes & dislikes for each video and their video names':
    Query6="""select likes as likecount,title as videotitle from videos"""
    cursor.execute(Query6)
    myconnection.commit()
    t6=cursor.fetchall()
    df6= pd.DataFrame(t6,columns=["likecount","video title"])
    st.write(df6)

elif questions == '7.Total number of views for each channel, and their channel names':
    Query7="""select channel_name as channelname,channel_viewcnt as totalviews from chanels"""
    cursor.execute(Query7)
    myconnection.commit()
    t7=cursor.fetchall()
    df7= pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif questions == '8.Names of all the channels that have published videos in the year 2022':
    Query8="""select title as video_title,published_date as videorelease,channel_name as channelname from videos
            where extract(year from published_date)=2022"""
    cursor.execute(Query8)
    myconnection.commit()
    t8=cursor.fetchall()
    df8= pd.DataFrame(t8,columns=["video title","published_date","channel name"])
    st.write(df8)

elif questions == '9.Average duration of all videos in each channel, and their channel names':
    Query9="""select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name """
    cursor.execute(Query9)
    myconnection.commit()
    t9=cursor.fetchall()
    df9= pd.DataFrame(t9,columns=["channelname","averageduration"])

    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif questions == '10.Videos with highest number of comments, and their channel names':
    Query10="""select title as videotitle,channel_name as channelname,comments as comments from videos where comments is
            not null order by comments desc"""
    cursor.execute(Query10)
    myconnection.commit()
    t10=cursor.fetchall()
    df10= pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)

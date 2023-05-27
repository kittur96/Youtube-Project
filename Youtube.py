from googleapiclient.discovery import build
import streamlit as st
from sqlalchemy.orm import sessionmaker
import pymongo
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

# Define the YouTube API service
api_key = "AIzaSyDt0aVidAwrXdu16L20JDGDpMaZBOiEhwc"
youtube = build('youtube', 'v3', developerKey=api_key)


# Function for getting channel details from youtube...
def get_channel_videos(channel_id):
    all_data=[]
    request =youtube.channels().list(
             part ='snippet,contentDetails,statistics',
             id= channel_id
    )

    response = request.execute()
    for i in response['items']:    
        data = {"Channel_name" :  i['snippet']['title'],
                "Subscribers"  :  i['statistics']['subscriberCount'],
                "Views"        :  i['statistics']['viewCount'],
                "Total_videos" :  i['statistics']['videoCount'],
                "Decription"   :  i['snippet']['description'],
                "PublishedAT"  :  i['snippet']['publishedAt'],
                 "playlist_id" :  i['contentDetails']['relatedPlaylists']['uploads'],
                 "Channel_id"  :  i['id']
               }
                
        
        all_data.append(data)
    
    return all_data   

# function for getting playlists ids of a particular channel...
def playlist_ids(channel_id):
    all_playlist_ids = []
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId = channel_id,
        maxResults=50
    )
    response = request.execute()
    for i in response['items']:
        play_list_data = {"Playlist_id":i['id'],
                          "Channel_id" :i['snippet']['channelId'],
                          "Title"      :i['snippet']['title'],
                          "Channel_Title":i['snippet']['channelTitle'],
                          "Item_Counts" :i['contentDetails']['itemCount']
                         }
        all_playlist_ids.append(play_list_data)
    return all_playlist_ids   
    
    
# Function for getting video ids.....
def get_video_ids(youtube,playlist_id):
    video_ids = []
    request =youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId = playlist_id,
            maxResults=50
    ) 
    response = request.execute()
    
    for i in response['items']:
        video_ids.append(i['contentDetails']['videoId'])
        
    next_page_token = response.get("nextPageToken")
    while next_page_token is not None:
        request =youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId = playlist_id,
            maxResults=50,
            pageToken =next_page_token)
        response = request.execute()  
            
        for i in response['items']:
             video_ids.append(i['contentDetails']['videoId'])    
        next_page_token = response.get("nextPageToken")
    return video_ids

# Function for getting all the video details from the searched channel...
def get_video_details(youtube, video_ids):
        all_video_stats=[]
        
        for i in range(0,len(video_ids),50):
                request =youtube.videos().list(
                     part ='snippet,statistics,contentDetails',
                     id =','.join(video_ids[i:i+50]))
                response = request.execute()
                

                for video in response['items']:
                        video_stats = {'snippet':['title','description','publishedAt','channelId'],
                                        'statistics':['viewCount','likeCount','commentCount'],
                                       'contentDetails':['duration','definition']
                                      }
                        video_info={}
                        video_info['video_id']=video['id']
                        for k in video_stats.keys():
                            for v in video_stats[k]:
                                try:
                                    video_info[v]=video[k][v]
                                except:
                                    video_info[v]=None
                                    
                        all_video_stats.append(video_info)
        return  all_video_stats

# function for getting comments from all the videos...
def get_comments_in_video(youtube,video_ids):
    all_comments = []
    
    for i in video_ids:
        request = youtube.commentThreads().list(
           part = "snippet,replies",
            videoId = i,
            maxResults =50
         )

        try:
                response = request.execute()
                for comment in response['items']:
                    get_comments_in_video = {"comment_id":comment["snippet"]["topLevelComment"]["id"],
                                        "comment_text":comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                        "comment_Author":comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],  
                                        "comment_publishedAt":comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],   
                                          "video_id" :comment['snippet']['videoId'] 
                                            }
                    all_comments.append(get_comments_in_video)
        
        except: 
             pass 
    return all_comments    

# Connect to MongoDB and retrieve the channel data
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['channel_data']


# Create a MySQL database connection
database_connection_url = 'mysql+pymysql://{username}:{password}@{host}/{database_name}'
connection_string = database_connection_url.format(
username = 'root',
password = '12345',
host = 'localhost',
database_name = 'yt'
)
engine = sqlalchemy.create_engine(connection_string)

Session = sessionmaker(bind=engine)
session = Session()
  
# Creating streamlit application...
def app():
    options =[] 
    st.markdown("<h1 style=: center;'>Fetching Youtube Data</h1>", unsafe_allow_html=True)
    channel_id =st.text_input("Enter :red[Y]T channel ID :smile: ")
    button = st.button("Fetch")     
    if button:
            with st.spinner('Fetching data from youtube...'):                
                plyid = get_channel_videos(channel_id)
                if plyid:
                    all_playlist_ids     = playlist_ids(channel_id)
                    unique_video_ids = get_video_ids(youtube,plyid[0]['playlist_id'])
                    vid_details      =get_video_details(youtube,unique_video_ids)
                    comments = get_comments_in_video(youtube,unique_video_ids)
                    st.success('Data successfully fetched from youtube to Mongodb')
                    # Coverting whole data into dictationary to move into mongodb
                    final_dic ={"Channel_Data":plyid,
                            "video_details":vid_details,
                            "playlist_ids_details":all_playlist_ids,
                            "comment_details":comments
                            }
                    collection.insert_one(final_dic)                    
                    st.write(final_dic)
                       
    if channel_id:
        options = [option.strip()  for option in channel_id.split(",")]
        selected_option = st.selectbox("select a channels",options) 
    mysqlbtn= st.button("Fetch data from mongodb to mysql")                              
    if mysqlbtn:
              with st.spinner('Transferring data from mongodb to mysql creating dataframe...'):   
                    st.success("Data Moved to MYsql database successfully...")
                    if selected_option:
                     all_data = collection.find()

                    for doc in all_data:
             #  Create DataFrame for channel details,video_details,playlist details and comment details...
                      channel_df = pd.DataFrame(doc['Channel_Data'])
                      videos_df = pd.DataFrame(doc['video_details'])
                      playlist_df  =pd.DataFrame(doc['playlist_ids_details'])
                      comments_df = pd.DataFrame(doc['comment_details'])

                    st.write(channel_df)
                    st.write(videos_df)
                    st.write(comments_df)
                    st.write(playlist_df)
                    
             # Transferring data from mongodb after converting them into dataframe to mysql database...
                    channel_df.to_sql('channel',con=engine , if_exists='append',index=False)
                    videos_df.to_sql('videos', con=engine , if_exists='append',index=False)
                    comments_df.to_sql('comments',con=engine , if_exists='append',index=False)
                    playlist_df.to_sql('playlists',con=engine , if_exists='append',index=False)

                    engine.dispose()   

 # Sidebar codings...   
button= st.sidebar.button('Hit button to convert the datatypes of mysql columns')  
if button:
    st.sidebar.success("datatypes has been changed successfully...")
    q1 =text('alter table channel modify column Subscribers int,modify column Views int,modify column Total_videos int')
    result1 =session.execute(q1)
    q2 = text('alter table videos modify column viewCount int,modify column likeCount int,modify column commentCount int')
    result1 =session.execute(q2)

    session.close()

st.sidebar.title("Select the questions and get the answer in the form of tables")
sidebar_space = st.sidebar.empty()

# Creating dropdowns for multiple questions
options=['What are the names of all the videos and their corresponding channels?',
                                    'Which channel have the most number of videos, and how many videos do they have?',
                                    'What are the top 10 most viewed videos and their respective channels?',
                                    'How many comments were made on each video, and what are their corresponding video names?',
                                    'Which videos have the highest number of likes, and what are their corresponding channel names?',
                                    'What is the total number of likes for each video, and what are their corresponding video names?',
                                    'What is the total number of views for each channel, and what are their corresponding channel names?',
                                    'What are the names of all the channels that have published videos in the year 2022?',
                                    'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                    'Which videos have the highest number of comments, and what are their corresponding channel names?',
                                    'None']

selected_option = sidebar_space.selectbox("choose the questions",options,index=10)
if selected_option =='None':
      pass #    st.write("select any question, you haven't selected any question")

elif  selected_option == 'What are the names of all the videos and their corresponding channels?':
    st.write('What are the names of all the videos and their corresponding channels?')
    query1 = "SELECT videos.title, channel.Channel_name from videos inner join channel ON channel.Channel_id = videos.channelId"
    df1 =pd.read_sql_query(query1,con =engine)
    st.table(df1)

elif selected_option == 'Which channel have the most number of videos, and how many videos do they have?':
    st.write('Which channel have the most number of videos, and how many videos do they have?')
    query2 = "SELECT Channel_name,Total_videos from channel order by Total_videos desc LIMIT 1 "
    df2 =pd.read_sql_query(query2,con =engine)
    st.table(df2)

elif selected_option == 'What are the top 10 most viewed videos and their respective channels?':
    st.write('What are the top 10 most viewed videos and their respective channels?')
    query3 = "SELECT videos.title,videos.viewCount,channel.Channel_name from videos inner join channel ON channel.Channel_id = videos.channelId order by viewCount desc LIMIT 10 "
    df3 =pd.read_sql_query(query3,con =engine)
    st.table(df3)
     
elif selected_option == 'How many comments were made on each video, and what are their corresponding video names?':
    st.write('How many comments were made on each video, and what are their corresponding video names?')
    query4 = "SELECT videos.title,count(comment_text) as total_comments from comments inner join videos on comments.video_id = videos.video_id group by videos.title"
    df4 =pd.read_sql_query(query4,con =engine)
    st.table(df4)

elif selected_option == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
    st.write('Which videos have the highest number of likes, and what are their corresponding channel names?')
    query5 = "SELECT channel.Channel_name ,videos.likeCount from videos inner join channel on channel.Channel_id = videos.channelId order by likeCount desc"
    df5 =pd.read_sql_query(query5,con =engine)
    st.table(df5)

elif selected_option == 'What is the total number of likes for each video, and what are their corresponding video names?':
    st.write('What is the total number of likes for each video, and what are their corresponding video names?')
    query6 = "SELECT title,likeCount from videos"
    df6 =pd.read_sql_query(query6,con =engine)
    st.table(df6)

elif selected_option == 'What is the total number of views for each channel, and what are their corresponding channel names?':
    st.write('What is the total number of views for each channel, and what are their corresponding channel names?')
    query7 = "SELECT Channel_name, Views from channel"
    df7 =pd.read_sql_query(query7,con =engine)
    st.table(df7)

elif selected_option == 'What are the names of all the channels that have published videos in the year 2022?':
    st.write('What are the names of all the channels that have published videos in the year 2022?')
    query8 = "SELECT Channel_name from channel inner join videos on channel.Channel_id = videos.channelId where SUBSTRING(videos.PublishedAT, 1, 4) ='2022'"
    df8 =pd.read_sql_query(query8,con =engine)
    st.table(df8)

elif selected_option == 'What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    st.write('What is the average duration of all videos in each channel, and what are their corresponding channel names?')
    query9 = "SELECT CASE WHEN duration LIKE '%%M%%' THEN REPLACE(SUBSTRING(duration, LOCATE('M', duration) - 2, 2), 'T', '0') * 60 + REPLACE(SUBSTRING(duration, LOCATE('S', duration) - 2, 2), 'T', '0')WHEN duration LIKE '%%S%%' THEN REPLACE(SUBSTRING(duration, LOCATE('S', duration) - 2, 2), 'T', '0') ELSE 0 END as AVG_DUR from videos"
    df9 =pd.read_sql_query(query9,con =engine)
    st.table(df9)

else:
    st.write('Which videos have the highest number of comments, and what are their corresponding channel names?')
    query10 = "SELECT channel.Channel_name,videos.title,videos.commentCount from channel inner join videos on videos.channelId = channel.Channel_id order by commentCount desc"
    df10 =pd.read_sql_query(query10,con =engine)
    st.table(df10)         
# Mainfunction execution of program will starts from here only....          
if __name__ == '__main__':
    app()                    
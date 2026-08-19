import socket
from confluent_kafka import Producer, KafkaError
import os
import time 
from googleapiclient.discovery import build
import json

# Kafka settings
BROKER = 'localhost:9092'  
TOPIC = 'youtube_topic_yiyinz'  # Replace with my Kafka topic
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="cmu-class.json"
youtube = build('youtube', 'v3')
# Example video ID, i will change
VIDEO_IDS = [
    'xoZC4zffCHo',
    'Tk0KpOCNb4Q',
    'zZf8ZRRHkmo' 
]
# Function to get the total likes of the comments of a video
## Reference: https://developers.google.com/youtube/v3/docs/commentThreads/list#properties
def video_total_likes_comment(video_id):
    total_likes = 0
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        textFormat='plainText',
        maxResults=100 ## Reference: https://developers.google.com/youtube/v3/docs/commentThreads/list#properties
    )
    
## Rference: https://medium.com/@rodolfo.antonio.sep/extracting-youtube-comments-with-python-a-detailed-guide-105363507a93
    while request:
        response = request.execute()
        for item in response['items']:
            like = item['snippet']['topLevelComment']['snippet']['likeCount']
            if like > 0:
                total_likes = total_likes + like
        request = youtube.commentThreads().list_next(request, response) ## Reference
    return total_likes

# Function to get the title of a video
def video_title(video_id):
    response = youtube.videos().list(
        part='snippet',
        id=video_id
    ).execute()
    title = response['items'][0]['snippet']['title']
    return title

# Function to create a Kafka producer
def create_kafka_producer(broker):
    conf = {
        'bootstrap.servers': broker,
        'client.id': socket.gethostname()
    }
    producer = Producer(conf)
    return producer

# Main function to stream YouTube likes
def stream_youtube_likes(video_ids):
    producer = create_kafka_producer(BROKER)
    while True: 
        for vid in video_ids:
            title = video_title(vid)
            likes = video_total_likes_comment(vid)
            message = f"{title}|{vid}|{likes}"
            producer.produce(TOPIC, key=vid, value=message)
        producer.flush()
        time.sleep(180) 

if __name__ == "__main__":
    stream_youtube_likes(VIDEO_IDS)
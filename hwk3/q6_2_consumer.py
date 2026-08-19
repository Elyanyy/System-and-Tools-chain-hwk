from confluent_kafka import Consumer, KafkaError
import socket

# Kafka settings
BROKER = 'localhost:9092'  # Change this to your Kafka broker address
GROUP_ID = 'analytics'
TOPIC = 'youtube_topic_yiyinz'  # Replace with your Kafka topic

# Function to create a Kafka consumer
def create_kafka_consumer(broker, group_id, topic):
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    return consumer

# Display data from Kafka
def display_kafka_data():
    consumer = create_kafka_consumer(BROKER, GROUP_ID, TOPIC)    
    print("Listening to Kafka topic:", TOPIC)

    while True:
        round_results=[]  

        while True:
            msg = consumer.poll(timeout=1.0)       
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Error:", msg.error())
                    break
            
            # message is in the form: title|id|likes
            value = msg.value().decode('utf-8')
            parts = value.split("|")
            if len(parts) == 3:
                title, vid, likes=parts[0], parts[1], int(parts[2])
                round_results.append((title, vid, likes))

        # the output of every round
        if round_results:
            print("\n--- Round Results ---")
            for title, vid, likes in round_results:
                print("Youtube:", title, "| ID:", vid, "| Total likes:", likes)

            # find the most popular video
            most_popular_video=round_results[0]
            for item in round_results:
                if item[2] > most_popular_video[2]:
                    most_popular_video= item

            print("\nThe most popular video so far is:")
            print("Title:", most_popular_video[0], "(ID:", most_popular_video[1], ")")
            print("Total likes:", most_popular_video[2], "\n")


    consumer.close()

if __name__== "__main__":
    # Streamlit app title
    print("Kafka Streamlit Consumer")
    display_kafka_data()
import os
from confluent_kafka import SerializingProducer
import simplejson
from datetime import datetime , timedelta
import random
import uuid
import json
import time

LONDON_COORDINATES = {"Latitude":51.5074, "Longitude":-0.1278}
BIRMINGHAM_COORDINATES={"Latitude":52.4862, "Longitude":-1.8904}

#Calculate movement increments
LATITUDE_INCREMENT=(BIRMINGHAM_COORDINATES['Latitude']-LONDON_COORDINATES['Latitude'])/100
LONGITUDE_INCREMENT=(BIRMINGHAM_COORDINATES['Longitude']-LONDON_COORDINATES['Longitude'])/100

#Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS=os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
VEHIVLE_TOPIC= os.getenv('VEHICLE_TOPIC','vehicle_data')
GPS_TOPIC= os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC= os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC= os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC= os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()
random.seed(42)


def generate_gps_data(device_id, timestamp, vehvile_type='private'):
   return{
      'id':uuid.uuid4(),
      'device_id': device_id,
      'timestamp': timestamp,
      'speed': random.uniform(0, 40), # km/h
      'direction': 'North-East',
      'vehicleType': vehvile_type
   }

def generate_traffic_camera_Data(device_id, timestamp,location,camera_id):
   return{
      'id':uuid.uuid4(),
      'device_id': device_id,
      'camera_id': camera_id,
      'location': location,
      'timestamp': timestamp,
      'snapshot': 'Base64EncodedString'
   }

def generate_weather_data(device_id, timestamp,location):
   return{
      'id':uuid.uuid4(),
      'device_id': device_id,
      'location': location,
      'timestamp': timestamp,
      'temperature': random.uniform(-5,26),
      'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rainy', 'Snowy']),
      'precipitation': random.uniform(0, 25),
      'windspeed': random.uniform(0,100),
      'humidity': random.randint(0,100),
      'airQualityIndex': random.uniform(0,500)
   }

def generate_emergency_incident_data(device_id, timestamp,location):
   return{
      'id':uuid.uuid4(),
      'device_id': device_id,
      'incidentid': uuid.uuid4(),
      'type':random.choice(['Accident','Fire', 'Medical', 'Police', 'None']),
      'location': location,
      'timestamp': timestamp,
      'status': random.choice(['Acitve','Resolved']),
      'description': 'Description of the incident'
   }

def get_next_time():
   global start_time
   start_time += timedelta(seconds=random.randint(30, 60 )) #update frequency for the time
   return start_time


def simulate_vehicle_movement():
   global start_location

   #move towards birmingham
   start_location['Latitude']+= LATITUDE_INCREMENT
   start_location['Longitude']+= LONGITUDE_INCREMENT

   #add some randomness
   start_location['Latitude']+= random.uniform(-0.0005, 0.0005)
   start_location['Longitude']+= random.uniform(-0.0005, 0.0005)
   return start_location

def generate_vehicle_data(device_id):
   location = simulate_vehicle_movement()
   return{
      'id':uuid.uuid4(),
      'device_id':device_id,
      'timestamp': get_next_time().isoformat(),
      'location': (location['Latitude'],location['Longitude']),
      'speed': random.uniform(10, 40),
      'direction': 'North-East',
      'make': 'BMW',
      'model': 'C500',
      'year': 2024,
      'fueltype': 'Hybrid'

   }

def json_serializer(obj):
   if isinstance(obj,uuid.UUID):
      return str(obj)

   raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
   if err is not  None:
      print(f'Message delivery failed: {err}')
   else:
      print(f'Mesage is delivered to {msg.topic()}[{msg.partition()}]')



def produce_data_to_kafka(producer, topic, data):
   producer.produce(
      topic,
      key = str(data['id']),
      value= json.dumps(data,default= json_serializer).encode('utf-8'),
      on_delivery = delivery_report
   )
   producer.flush()

def simulate_journey(producer, device_id):
   while True:
      vehicle_data = generate_vehicle_data(device_id)
      gps_data = generate_gps_data(device_id,vehicle_data['timestamp'])
      traffic_camera_data = generate_traffic_camera_Data(device_id,vehicle_data['timestamp'],vehicle_data['location'],'NikonCam123')
      weather_Data = generate_weather_data(device_id, vehicle_data['timestamp'],vehicle_data['location'])
      emergency_incident_data= generate_emergency_incident_data(device_id,vehicle_data['timestamp'],vehicle_data['location'])
      
      if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['Latitude'] 
          and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['Longitude'] ):
         print('Vehicle has been reached to Birmingham. Simulation ending...')
         break
            

      produce_data_to_kafka(producer,VEHIVLE_TOPIC,vehicle_data)
      produce_data_to_kafka(producer,GPS_TOPIC,gps_data)
      produce_data_to_kafka(producer,TRAFFIC_TOPIC,traffic_camera_data)
      produce_data_to_kafka(producer,WEATHER_TOPIC,weather_Data)
      produce_data_to_kafka(producer,EMERGENCY_TOPIC,emergency_incident_data)

      time.sleep(1)




if __name__ == "__main__":
   producer_config ={
    'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS,
    'error_cb': lambda err: print(f'KAFKA error: {err}'),
 }
   producer= SerializingProducer(producer_config)

   try:
    simulate_journey(producer, 'Vehicle-Abdullah')

   except KeyboardInterrupt:
    print('Simulation ended by the user')

   except Exception as e:
    print(f'Unexpected error occurred: {e}')







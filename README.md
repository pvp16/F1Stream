# F1Stream (Formula 1 Real-Time Pipeline)
A Formula 1 car with approximately 300 sensors built into it generates about 3 GB of data per lap and over 2 TB of data per race, the data sent by the car must be captured and processed in real-time,
this project simulates such a race session to capture, process, analyze and visualize telemetry data sent by a car in real-time, 
it's built using Kafka, Spark Streaming, Cassandra and PowerBI. 
## Architecture
1. The F1 telemetry data is fetched from [**FastF1 API**](https://theoehrly.github.io/Fast-F1/) using Python 
2. The fetched data is further captured by a Kafka producer and written into a Kafka topic. Every 0.24 seconds a telemerty sample is captured and streamed from each car.
3. The data is consumed from Kafka, transformed and written into Cassandra using Spark Structured Streaming.
4. A connection is setup between Cassandra and PowerBI desktop to perform visualizations and generate reports, the reports can be refreshed to obtain near real-time visualizations.
 
![architecture](https://github.com/pvp16/F1Stream/blob/master/images/StreamArch.png?raw=true)

## PowerBI Visualizations

![vis1](https://github.com/pvp16/F1Stream/blob/master/images/vis2.png?raw=true)

![vis2](https://github.com/pvp16/F1Stream/blob/master/images/vis1.png?raw=true)


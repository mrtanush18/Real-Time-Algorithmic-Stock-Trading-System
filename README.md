# Real Time Algorithmic Stock Trading System

SETUP AND EXECUTION  
This project was developed and tested using Python 3.11.1.

📌 PREREQUISITES  
+ 🐍 Python 3.11.1  
+ 🐳 Docker and Docker Compose  

INSTALLATION  
Install Docker if it is not already installed.

Install Python dependencies:  
+ `pip install -r requirements.txt`  
+ `python -m nltk.downloader vader_lexicon`  

Build and start all required services:  
+ `docker compose up -d --build`  

RUNNING THE APPLICATION  
Start the Kafka producers in separate terminals:  
+ `python news-producer.py`  
+ `python prices-producer.py`  

Start the Flink SQL client:  
+ `docker compose run sql-client`  

Copy and execute all commands from the `ddl.sql` file to create the required tables and views needed for visualization.

Launch the Streamlit dashboard:  
+ `streamlit run app.py`

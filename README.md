# Real Time Algorithmic Stock Trading System

![Pipeline image](https://github.com/user-attachments/assets/80e03406-6ad2-4ed4-ac08-0c2433d5ec91)


## SETUP AND EXECUTION  
This project was developed and tested using Python 3.11.1.

## INSTALLATIONS  
Install Docker if it is not already installed.

Install Python dependencies:  
+ `pip install -r requirements.txt`  
+ `python -m nltk.downloader vader_lexicon`  

Build and start all required services:  
+ `docker compose up -d --build`  

## RUNNING THE APPLICATION  
Start the Kafka producers in separate terminals:  
+ `python news-producer.py`  
+ `python prices-producer.py`  

Then go back to the same terminal, where you started Docker & start the Flink SQL client:  
+ `docker compose run sql-client`  

Copy and execute all commands from the `ddl.sql` file to create the required tables and views needed for visualization.

Launch the Streamlit dashboard:  
+ `streamlit run app.py`

![Dashboard](https://github.com/user-attachments/assets/69c0aecf-8393-4123-a33c-48671cb5264e)


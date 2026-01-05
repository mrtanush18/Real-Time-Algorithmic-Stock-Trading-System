import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import random
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# ============================================
# STREAMLIT PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="News Sentiment vs. Price Analysis",
    layout="wide"
)

# ============================================
# KAFKA CONFIGURATION
# ============================================
BROKERS = ['localhost:9092']  # Or your Redpanda brokers: ['redpanda-1:29092', 'redpanda-2:29092']

# Company mapping from your market_news table
COMPANY_MAPPING = {
    'AAPL': 'Apple Inc.',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc.',
    'AMZN': 'Amazon.com Inc.',
    'TSLA': 'Tesla Inc.',
    'NVDA': 'NVIDIA Corporation',
    'META': 'Meta Platforms Inc.',
    'JPM': 'JPMorgan Chase & Co.',
    'BRK-B': 'Berkshire Hathaway Inc.',
    'UNH': 'UnitedHealth Group Inc.',
    'JNJ': 'Johnson & Johnson',
    'V': 'Visa Inc.',
    'PG': 'Procter & Gamble Co.',
    'XOM': 'Exxon Mobil Corporation',
    'MA': 'Mastercard Inc.',
    'HD': 'Home Depot Inc.',
    'CVX': 'Chevron Corporation',
    'ABBV': 'AbbVie Inc.',
    'PFE': 'Pfizer Inc.',
    'AVGO': 'Broadcom Inc.',
    'KO': 'Coca-Cola Company',
    'PEP': 'PepsiCo Inc.',
    'COST': 'Costco Wholesale Corporation',
    'CSCO': 'Cisco Systems Inc.',
    'DIS': 'Walt Disney Company',
    'WMT': 'Walmart Inc.',
    'MCD': "McDonald's Corporation",
    'CRM': 'Salesforce Inc.',
    'NFLX': 'Netflix Inc.',
    'ABT': 'Abbott Laboratories',
    'TMO': 'Thermo Fisher Scientific Inc.',
    'ACN': 'Accenture plc',
    'DHR': 'Danaher Corporation',
    'NEE': 'NextEra Energy Inc.',
    'TXN': 'Texas Instruments Inc.',
    'VZ': 'Verizon Communications Inc.',
    'NKE': 'Nike Inc.',
    'LIN': 'Linde plc',
    'PM': 'Philip Morris International Inc.',
    'UNP': 'Union Pacific Corporation',
    'LLY': 'Eli Lilly and Company',
    'HON': 'Honeywell International Inc.',
    'ORCL': 'Oracle Corporation',
    'SBUX': 'Starbucks Corporation',
    'AMD': 'Advanced Micro Devices Inc.',
    'IBM': 'International Business Machines Corporation',
    'AXP': 'American Express Company',
    'GS': 'Goldman Sachs Group Inc.',
    'BA': 'Boeing Company',
    'CAT': 'Caterpillar Inc.',
    'MMM': '3M Company',
    'JPM': 'JPMorgan Chase & Co.',
    'GE': 'General Electric Company',
    'F': 'Ford Motor Company',
    'GM': 'General Motors Company',
    'T': 'AT&T Inc.',
    'INTC': 'Intel Corporation',
    'CSCO': 'Cisco Systems Inc.',
    'QCOM': 'QUALCOMM Incorporated',
    'ADBE': 'Adobe Inc.',
    'PYPL': 'PayPal Holdings Inc.',
    'INTU': 'Intuit Inc.',
    'ISRG': 'Intuitive Surgical Inc.',
    'MDT': 'Medtronic plc',
    'AMGN': 'Amgen Inc.',
    'GILD': 'Gilead Sciences Inc.',
    'BMY': 'Bristol-Myers Squibb Company',
    'CI': 'Cigna Corporation',
    'ANTM': 'Anthem Inc.',
    'AET': 'Aetna Inc.',
    'CVS': 'CVS Health Corporation',
    'WBA': 'Walgreens Boots Alliance Inc.',
    'MRK': 'Merck & Co. Inc.',
    'PFE': 'Pfizer Inc.',
    'LLY': 'Eli Lilly and Company',
    'ABBV': 'AbbVie Inc.',
    'BIIB': 'Biogen Inc.',
    'REGN': 'Regeneron Pharmaceuticals Inc.',
    'VRTX': 'Vertex Pharmaceuticals Incorporated',
    'ALXN': 'Alexion Pharmaceuticals Inc.',
    'CELG': 'Celgene Corporation',
    'GILD': 'Gilead Sciences Inc.',
    'AMGN': 'Amgen Inc.'
}

# ============================================
# KAFKA CONSUMERS
# ============================================
@st.cache_resource
def create_consumer(topic, group_id='dashboard-group'):
    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=BROKERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
    except Exception as e:
        st.warning(f"⚠️ Could not connect to Kafka topic '{topic}': {e}")
        return None

# Create consumers for all topics
sentiment_consumer = create_consumer('sentiment-trend', 'sentiment-group')
price_consumer = create_consumer('stock-prices', 'prices-group')

# ============================================
# SESSION STATE INITIALIZATION
# ============================================
if 'kafka_data' not in st.session_state:
    st.session_state.kafka_data = {
        'sentiments': [],
        'prices': []
    }

if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = 'JPM'

if 'last_kafka_poll' not in st.session_state:
    st.session_state.last_kafka_poll = datetime.now() - timedelta(seconds=10)

# ============================================
# HELPER FUNCTIONS
# ============================================
def get_company_name(symbol):
    """Get company name from mapping or return symbol"""
    return COMPANY_MAPPING.get(symbol, f"{symbol} Corporation")

def poll_kafka_data():
    """Poll all Kafka topics for new data"""
    current_time = datetime.now()
    
    # Only poll every 2 seconds to avoid overwhelming
    if (current_time - st.session_state.last_kafka_poll).total_seconds() < 2:
        return
    
    st.session_state.last_kafka_poll = current_time
    
    # Poll sentiment data
    if sentiment_consumer:
        try:
            batch = sentiment_consumer.poll(timeout_ms=100)
            for topic_partition, messages in batch.items():
                for message in messages:
                    data = message.value
                    if data not in st.session_state.kafka_data['sentiments']:
                        st.session_state.kafka_data['sentiments'].append(data)
        except:
            pass
    
    # Poll price data
    if price_consumer:
        try:
            batch = price_consumer.poll(timeout_ms=100)
            for topic_partition, messages in batch.items():
                for message in messages:
                    data = message.value
                    if data not in st.session_state.kafka_data['prices']:
                        st.session_state.kafka_data['prices'].append(data)
        except:
            pass

def get_current_price(symbol):
    """Get current price from Kafka data or use default"""
    prices = st.session_state.kafka_data['prices']
    if prices:
        for price_data in reversed(prices):
            if price_data.get('symbol') == symbol:
                return price_data.get('close', 100.0)
    
    # Default prices if no Kafka data
    default_prices = {
        'JPM': 171.98, 'AAPL': 182.64, 'MSFT': 379.43,
        'GOOGL': 136.39, 'TSLA': 243.93, 'NVDA': 494.62,
        'META': 338.95
    }
    return default_prices.get(symbol, 100.0)

def get_price_history(symbol, hours=24):
    """Get historical price data for selected symbol"""
    prices = st.session_state.kafka_data['prices']
    symbol_prices = [p for p in prices if p.get('symbol') == symbol]
    
    if not symbol_prices:
        # Generate sample data if no Kafka data
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        time_points = pd.date_range(start=start_time, end=end_time, periods=100)
        
        base_price = get_current_price(symbol)
        price_changes = np.random.normal(0, 0.005, 100).cumsum()
        prices_list = base_price * (1 + price_changes)
        
        return {
            'times': time_points,
            'prices': prices_list
        }
    
    # Use actual Kafka data
    df_prices = pd.DataFrame(symbol_prices)
    df_prices['timestamp'] = pd.to_datetime(df_prices.get('timestamp', pd.NaT))
    df_prices = df_prices.sort_values('timestamp')
    
    return {
        'times': df_prices['timestamp'].values,
        'prices': df_prices['close'].values
    }

def get_sentiment_history(symbol, hours=24):
    """Get historical sentiment data for selected symbol"""
    sentiments = st.session_state.kafka_data['sentiments']
    symbol_sentiments = [s for s in sentiments if s.get('symbol') == symbol]
    
    if not symbol_sentiments:
        # Generate sample data if no Kafka data
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        time_points = pd.date_range(start=start_time, end=end_time, periods=100)
        
        sentiment_base = np.sin(np.linspace(0, 4*np.pi, 100)) * 0.05
        sentiment_noise = np.random.normal(0, 0.02, 100)
        sentiments_list = sentiment_base + sentiment_noise
        
        return {
            'times': time_points,
            'sentiments': sentiments_list
        }
    
    # Use actual Kafka data
    df_sentiments = pd.DataFrame(symbol_sentiments)
    if 'window_start' in df_sentiments.columns:
        df_sentiments['timestamp'] = pd.to_datetime(df_sentiments['window_start'])
    elif 'timestamp' in df_sentiments.columns:
        df_sentiments['timestamp'] = pd.to_datetime(df_sentiments['timestamp'])
    else:
        df_sentiments['timestamp'] = pd.date_range(end=datetime.now(), periods=len(df_sentiments), freq='1h')
    
    df_sentiments = df_sentiments.sort_values('timestamp')
    
    return {
        'times': df_sentiments['timestamp'].values,
        'sentiments': df_sentiments['avg_sentiment'].values if 'avg_sentiment' in df_sentiments.columns else np.zeros(len(df_sentiments))
    }

# ============================================
# DASHBOARD UI
# ============================================
st.title("📊 News Sentiment vs. Price Analysis Dashboard")

# Poll Kafka data
poll_kafka_data()

# Dashboard Controls
st.markdown("### Dashboard Controls")

col1, col2, col3 = st.columns(3)

with col1:
    # Get unique symbols from Kafka data
    available_symbols = set()
    for sentiment in st.session_state.kafka_data['sentiments']:
        symbol = sentiment.get('symbol')
        if symbol:
            available_symbols.add(symbol)
    
    for price in st.session_state.kafka_data['prices']:
        symbol = price.get('symbol')
        if symbol:
            available_symbols.add(symbol)
    
    # Add default symbols if no Kafka data
    if not available_symbols:
        available_symbols = list(COMPANY_MAPPING.keys())[:10]
    
    selected_symbol = st.selectbox(
        "Select Symbol:",
        options=sorted(list(available_symbols)),
        index=list(available_symbols).index(st.session_state.selected_symbol) if st.session_state.selected_symbol in available_symbols else 0
    )
    st.session_state.selected_symbol = selected_symbol

with col2:
    time_range = st.selectbox(
        "Time Range:",
        ["Last 24 Hours", "Last Week", "Last Month", "Last 3 Months"],
        index=0
    )

with col3:
    # Kafka status
    total_messages = len(st.session_state.kafka_data['sentiments']) + len(st.session_state.kafka_data['prices'])
    st.caption(f"Connected: {sentiment_consumer is not None and price_consumer is not None}")

# Company Information Section
company_name = get_company_name(selected_symbol)
current_price = get_current_price(selected_symbol)

st.markdown("---")
st.markdown(f"### {company_name} ({selected_symbol})")

# Display company metrics
info_cols = st.columns(4)
with info_cols[0]:
    st.metric("Current Price", f"${current_price:.2f}")
with info_cols[1]:
    st.metric("Sector", "Financial Services" if selected_symbol == "JPM" else "Technology")
with info_cols[2]:
    company_info = {
        'JPM': 'Banks - Diversified',
        'AAPL': 'Consumer Electronics',
        'MSFT': 'Software - Infrastructure',
        'GOOGL': 'Internet Content & Information',
        'TSLA': 'Auto Manufacturers',
        'NVDA': 'Semiconductors',
        'META': 'Internet Content & Information'
    }
    st.metric("Industry", company_info.get(selected_symbol, "N/A"))
with info_cols[3]:
    market_caps = {
        'JPM': '$485B', 'AAPL': '$2.8T', 'MSFT': '$2.8T',
        'GOOGL': '$1.7T', 'TSLA': '$775B', 'NVDA': '$1.2T',
        'META': '$880B'
    }
    st.metric("Market Cap", market_caps.get(selected_symbol, "N/A"))

# Sentiment vs Price Visualization
st.markdown("---")
st.markdown("### Sentiment vs. Price Correlation")

# Get data based on time range
hours_map = {
    "Last 24 Hours": 24,
    "Last Week": 168,
    "Last Month": 720,
    "Last 3 Months": 2160
}
hours = hours_map.get(time_range, 24)

price_data = get_price_history(selected_symbol, hours)
sentiment_data = get_sentiment_history(selected_symbol, hours)

# Create visualization
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.1,
    subplot_titles=('News Sentiment Trend', 'Stock Price Movement'),
    row_heights=[0.4, 0.6]
)

# Add sentiment trace
fig.add_trace(
    go.Scatter(
        x=sentiment_data['times'],
        y=sentiment_data['sentiments'],
        mode='lines',
        name='Sentiment',
        line=dict(color='#3B82F6', width=2),
        fill='tozeroy',
        fillcolor='rgba(59, 130, 246, 0.2)'
    ),
    row=1, col=1
)

# Add price trace
fig.add_trace(
    go.Scatter(
        x=price_data['times'],
        y=price_data['prices'],
        mode='lines',
        name='Price',
        line=dict(color='#10B981', width=2)
    ),
    row=2, col=1
)

# Update layout
fig.update_layout(
    height=600,
    showlegend=True,
    hovermode='x unified',
    plot_bgcolor='white',
    paper_bgcolor='white',
    xaxis2=dict(title='Time'),
    yaxis1=dict(
        title='Sentiment Score',
        gridcolor='#E5E7EB'
    ),
    yaxis2=dict(
        title='Price ($)',
        gridcolor='#E5E7EB'
    ),
    legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01,
        bgcolor='rgba(255, 255, 255, 0.9)'
    )
)

# Display the plot
st.plotly_chart(fig, use_container_width=True)

# Correlation Metrics
st.markdown("### Correlation Analysis")

# Calculate correlation (aligning data by time)
# For simplicity, we'll use the last 100 data points
min_len = min(len(sentiment_data['sentiments']), len(price_data['prices']), 100)
if min_len > 1:
    sentiment_values = sentiment_data['sentiments'][-min_len:]
    price_values = price_data['prices'][-min_len:]
    correlation = np.corrcoef(sentiment_values, price_values)[0,1]
    
    # Calculate additional metrics
    sentiment_mean = np.mean(sentiment_values)
    sentiment_std = np.std(sentiment_values)
    price_change_pct = ((price_values[-1] - price_values[0]) / price_values[0]) * 100
else:
    correlation = 0
    sentiment_mean = 0
    sentiment_std = 0
    price_change_pct = 0

# Display metrics
metric_cols = st.columns(4)
with metric_cols[0]:
    st.metric(
        "Sentiment-Price Correlation",
        f"{correlation:.3f}",
        delta="Strong" if abs(correlation) > 0.5 else "Moderate" if abs(correlation) > 0.3 else "Weak",
        delta_color="normal" if correlation > 0 else "inverse"
    )

with metric_cols[1]:
    st.metric(
        "Avg Sentiment",
        f"{sentiment_mean:.4f}",
        delta="Positive" if sentiment_mean > 0.02 else "Negative" if sentiment_mean < -0.02 else "Neutral",
        delta_color="normal" if sentiment_mean > 0 else "inverse"
    )

with metric_cols[2]:
    st.metric("Sentiment Volatility", f"{sentiment_std:.4f}")

with metric_cols[3]:
    st.metric("Price Change", f"{price_change_pct:.2f}%")

# Data Information Footer
st.markdown("---")
footer_col1, footer_col2 = st.columns([2, 1])

with footer_col1:
    current_time = datetime.now().strftime('%H:%M:%S')
    st.caption(f"Last update: {current_time}")
    st.caption(f"Kafka topics: sentiment-trend, stock-prices")
    sentiment_count = len(st.session_state.kafka_data['sentiments'])
    price_count = len(st.session_state.kafka_data['prices'])
    st.caption(f"Data: {sentiment_count} sentiment records, {price_count} price records")

with footer_col2:
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

# Auto-refresh every 5 seconds
time.sleep(5)
st.rerun()
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime, timedelta
import time
from kafka import KafkaConsumer
import socket

# Configuration
CONFIG = {
    'kafka_servers': ['localhost:9092'],  
    'topics': {
        'joined': 'stock_news_joined',     
        'prices': 'stock-prices',          
        'news': 'market-news'              
    }
}

# Page configuration
st.set_page_config(
    page_title="Stock Dashboard - LIVE",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'stock_df' not in st.session_state:
    st.session_state.stock_df = pd.DataFrame()
if 'last_fetch' not in st.session_state:
    st.session_state.last_fetch = None
if 'topic_stats' not in st.session_state:
    st.session_state.topic_stats = {}

# Function to get available topics
def get_available_topics():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=CONFIG['kafka_servers'],
            api_version=(2, 8, 0),
            consumer_timeout_ms=2000
        )
        topics = list(consumer.topics())
        consumer.close()
        return topics
    except:
        return []

# Get available topics
available_topics = get_available_topics()

# Title
st.title("📊 LIVE Stock & News Dashboard")
st.markdown(f"Connected to: `{CONFIG['kafka_servers'][0]}`")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("🔌 Connection Status")
    
    if available_topics:
        st.success(f"✅ Connected!")
        st.caption(f"Found {len(available_topics)} topics")
        
        st.subheader("📁 Available Topics")
        for topic in sorted(available_topics):
            if topic in CONFIG['topics'].values():
                st.success(f"📊 {topic}")
            else:
                st.caption(f"• {topic}")
    else:
        st.error("❌ Not connected")
    
    st.divider()
    
    st.header("🎛️ Controls")
    
    # Topic selection based on what's available
    topic_options = []
    if 'stock_news_joined' in available_topics:
        topic_options.append("Joined Data (stock_news_joined)")
    if 'stock-prices' in available_topics:
        topic_options.append("Stock Prices (stock-prices)")
    if 'market-news' in available_topics:
        topic_options.append("Market News (market-news)")
    
    if topic_options:
        selected_topic_display = st.selectbox(
            "Data Source",
            topic_options,
            index=0
        )
        
        # Extract topic name from display
        if "stock_news_joined" in selected_topic_display:
            current_topic = "stock_news_joined"
        elif "stock-prices" in selected_topic_display:
            current_topic = "stock-prices"
        else:
            current_topic = "market-news"
    else:
        st.warning("No data topics available")
        current_topic = None
    
    # Symbol filter
    symbol = st.selectbox(
        "Stock Symbol",
        ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'ALL'],
        index=0
    )
    
    # Time filter
    time_filter = st.selectbox(
        "Time Range",
        ["Last 5 minutes", "Last 15 minutes", "Last hour", "Last 4 hours", "All data"],
        index=2
    )
    
    # Auto-refresh
    auto_refresh = st.checkbox("🔄 Auto-refresh", value=True)
    if auto_refresh:
        refresh_rate = st.slider("Refresh every (seconds)", 5, 60, 10)
    
    if st.button("📥 Fetch Data Now"):
        st.session_state.last_fetch = None
    
    st.divider()
    st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')}")

# Data loading function
def load_from_topic(topic_name, max_messages=100):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=CONFIG['kafka_servers'],
            auto_offset_reset='latest',
            consumer_timeout_ms=3000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id=f'streamlit-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
        )
        
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        
        consumer.close()
        
        if messages:
            df = pd.DataFrame(messages)
            
            # Clean column names (remove special characters)
            df.columns = [str(col).strip() for col in df.columns]
            
            # Convert timestamps
            for col in df.columns:
                col_lower = str(col).lower()
                if any(time_word in col_lower for time_word in ['time', 'timestamp', 'date']):
                    try:
                        if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                            # Handle milliseconds or seconds
                            if df[col].max() > 1e12:  # Likely milliseconds
                                df[col] = pd.to_datetime(df[col], unit='ms')
                            else:
                                df[col] = pd.to_datetime(df[col], unit='s')
                        else:
                            df[col] = pd.to_datetime(df[col])
                    except Exception as e:
                        st.warning(f"Could not convert column {col}: {e}")
            
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error loading {topic_name}: {str(e)[:100]}")
        return pd.DataFrame()

# Load data if we have a topic
if current_topic:
    should_fetch = (
        st.session_state.last_fetch is None or
        not auto_refresh or
        (datetime.now() - st.session_state.last_fetch).seconds > (refresh_rate if auto_refresh else 60)
    )
    
    if should_fetch:
        with st.spinner(f"Loading from {current_topic}..."):
            df = load_from_topic(current_topic, 200)
            
            if not df.empty:
                st.session_state.stock_df = df
                st.session_state.last_fetch = datetime.now()
                
                # Store topic stats
                st.session_state.topic_stats[current_topic] = {
                    'rows': len(df),
                    'columns': list(df.columns),
                    'sample': df.iloc[0].to_dict() if len(df) > 0 else {}
                }
                
                # Apply filters
                if symbol != 'ALL':
                    symbol_cols = [col for col in df.columns if 'symbol' in col.lower()]
                    if symbol_cols:
                        symbol_col = symbol_cols[0]
                        df = df[df[symbol_col] == symbol]
                
                # Apply time filter
                time_cols = [col for col in df.columns if any(word in col.lower() for word in ['time', 'timestamp'])]
                if time_cols and len(df) > 0:
                    time_col = time_cols[0]
                    
                    # Calculate cutoff based on selection
                    if time_filter == "Last 5 minutes":
                        cutoff = datetime.now() - timedelta(minutes=5)
                    elif time_filter == "Last 15 minutes":
                        cutoff = datetime.now() - timedelta(minutes=15)
                    elif time_filter == "Last hour":
                        cutoff = datetime.now() - timedelta(hours=1)
                    elif time_filter == "Last 4 hours":
                        cutoff = datetime.now() - timedelta(hours=4)
                    else:
                        cutoff = datetime(1970, 1, 1)  # All data
                    
                    df = df[df[time_col] > cutoff]

# Display main content
if not st.session_state.stock_df.empty:
    df = st.session_state.stock_df
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📈 Visualizations", "📋 Data View", "⚙️ System Info"])
    
    with tab1:
        # Determine what type of data we have
        is_joined_data = current_topic == 'stock_news_joined'
        has_prices = any(col in df.columns for col in ['close', 'close_price', 'price'])
        has_news = any(col in df.columns for col in ['headline', 'summary', 'sentiment'])
        
        if has_prices:
            # Find price column
            price_col = None
            for col in ['close_price', 'close', 'price', 'last']:
                if col in df.columns:
                    price_col = col
                    break
            
            # Find time column
            time_col = None
            for col in ['price_time', 'event_time', 'timestamp', 'time']:
                if col in df.columns:
                    time_col = col
                    break
            
            if price_col and time_col:
                st.subheader(f"{symbol} Price Chart")
                
                # Create price chart
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=df[time_col],
                    y=df[price_col],
                    mode='lines+markers',
                    name='Price',
                    line=dict(color='#1f77b4', width=2),
                    marker=dict(size=4)
                ))
                
                # Add news markers if available
                if 'sentiment' in df.columns and 'headline' in df.columns:
                    news_df = df[df['headline'].notna()].copy()
                    if not news_df.empty:
                        colors = []
                        for s in news_df['sentiment']:
                            if isinstance(s, (int, float)):
                                if s > 0.5:
                                    colors.append('darkgreen')
                                elif s > 0:
                                    colors.append('green')
                                elif s < -0.5:
                                    colors.append('darkred')
                                elif s < 0:
                                    colors.append('red')
                                else:
                                    colors.append('gray')
                            else:
                                colors.append('gray')
                        
                        fig.add_trace(go.Scatter(
                            x=news_df[time_col],
                            y=news_df[price_col],
                            mode='markers',
                            name='News Events',
                            marker=dict(
                                size=10,
                                color=colors,
                                symbol='diamond',
                                line=dict(width=1, color='white')
                            ),
                            text=news_df['headline'],
                            hovertemplate='<b>News</b><br>%{text}<br>Time: %{x}<br>Price: $%{y}<extra></extra>'
                        ))
                
                fig.update_layout(
                    title=f"{symbol} - {time_filter}",
                    xaxis_title="Time",
                    yaxis_title="Price ($)",
                    height=500,
                    template="plotly_white",
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Metrics row
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    latest_price = df[price_col].iloc[-1]
                    st.metric("Current Price", f"${latest_price:.2f}")
                
                with col2:
                    if len(df) > 1:
                        price_change = df[price_col].iloc[-1] - df[price_col].iloc[-2]
                        change_pct = (price_change / df[price_col].iloc[-2]) * 100
                        st.metric("Change", f"${price_change:.2f}", f"{change_pct:.1f}%")
                
                with col3:
                    if 'volume' in df.columns:
                        avg_volume = df['volume'].mean()
                        st.metric("Avg Volume", f"{int(avg_volume):,}")
                
                with col4:
                    if 'sentiment' in df.columns:
                        avg_sentiment = df['sentiment'].mean()
                        icon = "🚀" if avg_sentiment > 1 else "📈" if avg_sentiment > 0.5 else "↗️" if avg_sentiment > 0 else "➡️" if avg_sentiment == 0 else "↘️" if avg_sentiment > -0.5 else "📉" if avg_sentiment > -1 else "💥"
                        st.metric("Sentiment", f"{avg_sentiment:.2f}", icon)
        
        # News section
        if has_news:
            st.subheader("Recent News")
            
            # Filter for news items
            news_cols = [col for col in df.columns if col in ['headline', 'summary', 'sentiment', 'source']]
            if news_cols:
                news_df = df[news_cols].dropna(subset=['headline']).copy() if 'headline' in df.columns else pd.DataFrame()
                
                if not news_df.empty:
                    # Sort by time if available
                    if 'time' in df.columns or 'timestamp' in df.columns:
                        time_col = 'time' if 'time' in df.columns else 'timestamp'
                        if time_col in df.columns:
                            news_df = news_df.merge(df[[time_col]], left_index=True, right_index=True)
                            news_df = news_df.sort_values(time_col, ascending=False)
                    
                    # Display news cards
                    for _, row in news_df.head(10).iterrows():
                        with st.container():
                            col1, col2 = st.columns([4, 1])
                            
                            with col1:
                                # Headline
                                headline = str(row.get('headline', 'No headline'))
                                st.markdown(f"**{headline}**")
                                
                                # Summary
                                if 'summary' in row and pd.notna(row['summary']):
                                    st.write(str(row['summary'])[:150] + "...")
                                
                                # Metadata
                                meta_cols = st.columns(3)
                                with meta_cols[0]:
                                    if 'source' in row:
                                        st.caption(f"📰 {row['source']}")
                                with meta_cols[1]:
                                    if 'symbol' in row:
                                        st.caption(f"🏷️ {row['symbol']}")
                                with meta_cols[2]:
                                    if time_col in row:
                                        st.caption(f"🕒 {row[time_col].strftime('%H:%M')}")
                            
                            with col2:
                                # Sentiment indicator
                                sentiment = row.get('sentiment', 0)
                                if isinstance(sentiment, (int, float)):
                                    if sentiment > 0.5:
                                        st.success(f"😊 {sentiment:.1f}")
                                    elif sentiment > 0:
                                        st.success(f"🙂 {sentiment:.1f}")
                                    elif sentiment < -0.5:
                                        st.error(f"😟 {sentiment:.1f}")
                                    elif sentiment < 0:
                                        st.error(f"🙁 {sentiment:.1f}")
                                    else:
                                        st.info(f"😐 {sentiment:.1f}")
                            
                            st.divider()
    
    with tab2:
        st.subheader("Raw Data View")
        
        # Show data preview
        st.dataframe(
            df,
            use_container_width=True,
            height=400
        )
        
        # Statistics
        st.subheader("Dataset Statistics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Basic Info:**")
            st.write(f"- Total rows: `{len(df)}`")
            st.write(f"- Columns: `{len(df.columns)}`")
            st.write(f"- Memory usage: `{df.memory_usage(deep=True).sum() / 1024:.1f} KB`")
            
            if 'symbol' in df.columns:
                symbols = df['symbol'].unique()
                st.write(f"- Unique symbols: `{len(symbols)}`")
                st.write(f"- Symbols: `{', '.join(sorted(symbols[:5]))}`" + ("..." if len(symbols) > 5 else ""))
        
        with col2:
            st.write("**Column Details:**")
            for i, col in enumerate(df.columns[:8]):  # Show first 8 columns
                dtype = str(df[col].dtype)
                non_null = df[col].notna().sum()
                st.write(f"- `{col}`: {dtype}, {non_null}/{len(df)} non-null")
            
            if len(df.columns) > 8:
                st.caption(f"... and {len(df.columns) - 8} more columns")
        
        # Download option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with tab3:
        st.subheader("System Information")
        
        # Connection info
        st.write("**🔗 Connection Details:**")
        st.code(f"""
        Kafka Server: {CONFIG['kafka_servers'][0]}
        Current Topic: {current_topic}
        Total Topics: {len(available_topics)}
        Last Fetch: {st.session_state.last_fetch.strftime('%H:%M:%S') if st.session_state.last_fetch else 'Never'}
        Data Rows: {len(df)}
        """)
        
        # Topic stats
        st.write("**📊 Topic Statistics:**")
        for topic, stats in st.session_state.topic_stats.items():
            with st.expander(f"Topic: {topic}"):
                st.write(f"- Rows loaded: {stats['rows']}")
                st.write(f"- Columns: {len(stats['columns'])}")
                if stats['sample']:
                    st.write("- Sample data:")
                    st.json(stats['sample'])
            
        # Quick actions
        st.write("**⚡ Quick Actions:**")
        if st.button("🔄 Reload All Data"):
            st.session_state.last_fetch = None
            st.rerun()
        
        if st.button("📊 Check Topic Health"):
            st.info(f"Topic '{current_topic}' has {len(df)} rows, {len(df.columns)} columns")
            
else:
    # Welcome/help screen
    st.info("👋 Welcome to the Stock Dashboard!")
    
    if current_topic:
        st.warning(f"Topic `{current_topic}` is connected but has no data yet.")
    else:
        st.error("No data topic selected. Please select a topic from the sidebar.")

# Footer
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns(3)
with footer_col1:
    st.caption(f"Topic: {current_topic or 'None'}")
with footer_col2:
    if st.session_state.last_fetch:
        st.caption(f"Updated: {st.session_state.last_fetch.strftime('%H:%M:%S')}")
with footer_col3:
    st.caption(f"Rows: {len(st.session_state.stock_df)}")

# Auto-refresh
if auto_refresh and current_topic:
    time.sleep(refresh_rate)
    st.rerun()
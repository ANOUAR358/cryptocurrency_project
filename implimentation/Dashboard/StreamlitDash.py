import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import time
from streamlit_autorefresh import st_autorefresh


count = st_autorefresh(interval=30000, key="crypto_dashboard")

# Snowflake connection and data fetching function
#@st.cache_data(ttl=10)
def query_snowflake(query) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        user="ELKHOLTIHM",
        password="@Elkholtihm2002",
        account="qhoozoj-le22002",
        warehouse="COMPUTE_WH",
        database="CRYPTODW",
        schema="CRYPTOS"
    )
    data = pd.read_sql(query, conn)
    conn.close()
    return data

# Fetch data
st.title("Crypto Dashboard")
st.write("Real-time metrics for selected cryptocurrencies")

#------------------col to manage -------------------
cols1 = st.columns(5)
cols2 = st.columns(2)
cols3 = st.columns(2)
cols4 = st.columns(2)


# ----------------------sentiment -----------------------------
with cols1[4]:
    # Query to fetch sentiment data
    SENTIMENT_QUERY = """
        SELECT 
            timestamp,
            sentiment_score
        FROM sentiment_dim
        ORDER BY timestamp DESC
        LIMIT 1  -- Get the latest sentiment score
    """
    #st.cache_data.clear()
    # Fetch sentiment data
    with st.spinner('Fetching latest sentiment data...'):
        sentiment_data = query_snowflake(SENTIMENT_QUERY)

    if not sentiment_data.empty:
        # Get the latest sentiment score
        latest_sentiment = sentiment_data.iloc[0]['SENTIMENT_SCORE']
        timestamp = sentiment_data.iloc[0]['TIMESTAMP']

        # Determine the color and emoji based on the sentiment score
        if latest_sentiment > 0:
            color = "green"
            emoji = "😊"  # Happy emoji for positive sentiment
        elif latest_sentiment < 0:
            color = "red"
            emoji = "😞"  # Sad emoji for negative sentiment
        else:
            color = "gray"
            emoji = "😐"  # Neutral emoji for zero sentiment

        # Display the metric with styling
        st.markdown(
            f"""
            <div style="text-align: center; background-color: {color}; padding: 10px; border-radius: 5px;">
                <h1 style="color: white; font-size: 36px;">{emoji}</h1>
                <h2 style="color: white;">Sentiment Score</h2>
                <h3 style="color: white;">{latest_sentiment}</h3>
                <p style="color: white; font-size: 12px;">Last Updated: {timestamp}</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.warning("No sentiment data available.")

# ---------------------------------------metrics --------------------------------
# Query to fetch data for dashboard
Metric_QUERY = """
    SELECT 
        f.symbol, 
        f.price, 
        c.market_cap, 
        c.price_change_perc,
        c.market_cap_change
    FROM fact_table f
    JOIN crypto_dim c
        ON f.timestamp = c.timestamp AND f.symbol = c.symbol
    ORDER BY f.timestamp DESC
    LIMIT 4;
"""


with st.spinner('Fetching data from Snowflake...'):
    crypto_data = query_snowflake(Metric_QUERY)

# Display metrics
if not crypto_data.empty:
    for idx, row in crypto_data.iterrows():
        symbol = row['SYMBOL']
        price = row['PRICE']
        market_cap = row['MARKET_CAP']
        price_change_perc = row['PRICE_CHANGE_PERC']
        market_cap_change = row['MARKET_CAP_CHANGE']

        arrow = "\u2B06" if market_cap_change > 0 else "\u2B07"
        market_cap_color = "green" if market_cap_change > 0 else "red"  

        with cols1[idx]:
            st.metric(
                label=symbol,
                value=f"${price:,.2f}",
                delta=f"{price_change_perc:.2f}%",
                delta_color='normal'
            )
            # Write Market Cap with dynamic arrow and color
            st.write(
                f"<span style='color:{market_cap_color};'>"
                f"Market Cap: ${market_cap:,.0f} {arrow} ({market_cap_change})"
                f"</span>",
                unsafe_allow_html=True
            )
else:
    st.error("No data available")

st.success("Dashboard loaded successfully!")

# -------------------------------------crrelations ---------------------------------
CORRELATION_QUERY = """
    SELECT 
        correlations_dim.gold_price, 
        correlations_dim.interest_rate, 
        correlations_dim.stocks_price,
        fact_table.symbol, 
        fact_table.price
    FROM correlations_dim
    JOIN fact_table 
        ON correlations_dim.day = DAY(TO_DATE(fact_table.timestamp))
"""

with st.spinner('Fetching data for correlation matrix...'):
    correlation_data = query_snowflake(CORRELATION_QUERY)

if not correlation_data.empty:
    with cols2[1]:
        st.subheader("Correlation Matrix")

        # Pivot the data to have symbols as columns for correlation analysis
        pivot_data = correlation_data.pivot_table(
            index='GOLD_PRICE', 
            columns='SYMBOL', 
            values='PRICE',
            aggfunc='mean'
        )

        # Add STOCKS_PRICE and INTEREST_RATE to the pivoted data
        additional_metrics = correlation_data.groupby('GOLD_PRICE').mean(numeric_only=True)
        pivot_data = pivot_data.merge(
            additional_metrics[['STOCKS_PRICE', 'INTEREST_RATE']],
            on='GOLD_PRICE',
            how='left'
        )

        # Compute the correlation matrix
        correlation_matrix = pivot_data.corr()

        # Visualize the correlation matrix using a heatmap
        fig = px.imshow(
            correlation_matrix,
            text_auto=True,
            color_continuous_scale="RdBu",
            title="Correlation Matrix"
        )
        st.plotly_chart(fig, use_container_width=True)


#----------------------informations table------------------------------
crypto_info_QUERY = """select distinct * from crypto_info;"""
crypto_info = query_snowflake(crypto_info_QUERY)
with cols2[0]:
    st.subheader("Informations about Cryptocurrencies")
    st.dataframe(crypto_info, use_container_width=True)

# ------------------------crypto price evolution ----------------------
PRICE_EVOLUTION_QUERY = """
    SELECT 
        fact_table.timestamp,
        fact_table.symbol, 
        fact_table.price
    FROM fact_table
    ORDER BY fact_table.timestamp
"""

with st.spinner('Fetching data for crypto price evolution...'):
    price_data = query_snowflake(PRICE_EVOLUTION_QUERY)

with cols3[0]:
    st.subheader('Price over timestamp')
    if not price_data.empty:
        # Convert timestamp to datetime for proper plotting
        price_data['TIMESTAMP'] = pd.to_datetime(price_data['TIMESTAMP'])

        # Create a line chart with Plotly
        fig = px.line(
            price_data,
            x='TIMESTAMP',
            y='PRICE',
            color='SYMBOL',
            title="Cryptocurrency Price Evolution Over Time",
            labels={'PRICE': 'Price (USD)', 'TIMESTAMP': 'Timestamp', 'SYMBOL': 'Crypto Symbol'}
        )

        # Add chart to Streamlit
        st.plotly_chart(fig, use_container_width=True)
#--------------------------------dominanace ----------------------------------
DOMINANCE_QUERY = """
    SELECT 
        fact_table.symbol,
        fact_table.dominance
    FROM fact_table
    WHERE (fact_table.symbol, fact_table.timestamp) IN (
        SELECT 
            fact_table.symbol, 
            MAX(fact_table.timestamp) 
        FROM fact_table
        GROUP BY fact_table.symbol
    )
    ORDER BY fact_table.dominance DESC
"""

with cols3[1]:
    # Fetch data for dominance visualization
    with st.spinner('Fetching data for dominance pie chart...'):
        dominance_data = query_snowflake(DOMINANCE_QUERY)

    if not dominance_data.empty:
        # Create a pie chart with Plotly
        fig = px.pie(
            dominance_data,
            values='DOMINANCE',
            names='SYMBOL',
            title="Cryptocurrency Dominance Distribution (Latest Record)",
            labels={'DOMINANCE': 'Dominance (%)', 'SYMBOL': 'Crypto Symbol'},
            hole=0.4 
        )

        # Add chart to Streamlit
        st.plotly_chart(fig, use_container_width=True)

#----------------------------------exchange rate --------------------
with cols4[0]:
    st.subheader('Exchange rate for each crypto')
    EXCHANGE_QUERY = """
        SELECT 
            fact_table.symbol,
            fact_table.exchangerate
        FROM fact_table
        WHERE (fact_table.symbol, fact_table.timestamp) IN (
            SELECT 
                fact_table.symbol, 
                MAX(fact_table.timestamp) 
            FROM fact_table
            GROUP BY fact_table.symbol
        )
        ORDER BY fact_table.exchangerate DESC
    """
    # Fetch data for exchange rates
    with st.spinner('Fetching data for exchange rates histogram...'):
        exchange_data = query_snowflake(EXCHANGE_QUERY)

    if not exchange_data.empty:
        # Create a histogram of exchange rates
        fig = px.histogram(
            exchange_data,
            y='EXCHANGERATE',
            x = 'SYMBOL',
            title="Distribution of Exchange Rates by Cryptocurrency Symbol",
            labels={'EXCHANGERATE': 'Exchange Rate', 'SYMBOL': 'Crypto Symbol'},
            nbins=30  
        )

        # Add chart to Streamlit
        st.plotly_chart(fig, use_container_width=True)

#---------------------------------blockchain info --------------------
import streamlit as st
import plotly.express as px

import streamlit as st
import plotly.express as px

with cols4[1]:
    st.subheader('Exchange rate for each crypto')

    # Query to fetch blockchain data
    BLOCKCHAIN_QUERY = """
        SELECT 
            blockchain_info.timestamp,
            blockchain_info.symbol,
            blockchain_info.total_trans,
            blockchain_info.pts,
            blockchain_info.hashrate
        FROM blockchain_info
        ORDER BY timestamp DESC
    """
    with st.spinner('Fetching data for blockchain metrics evolution...'):
        blockchain_data = query_snowflake(BLOCKCHAIN_QUERY)

    if not blockchain_data.empty:
        # Add a select box for metrics
        metrics = ['TOTAL_TRANS', 'PTS', 'HASHRATE']
        selected_metric = st.selectbox(
            'Select a Metric to Display',
            options=metrics
        )

        # Prepare data for the selected metric
        if selected_metric in blockchain_data.columns:
            # Create the line chart for the selected metric across all symbols
            fig = px.line(
                blockchain_data,
                x='TIMESTAMP',
                y=selected_metric,
                color='SYMBOL',  # Differentiate by symbol
                title=f"{selected_metric} Across All Cryptocurrencies",
                labels={'TIMESTAMP': 'Timestamp', selected_metric: 'Value', 'SYMBOL': 'Crypto Symbol'}
            )

            # Update layout for better display
            fig.update_layout(
                height=600,
                width=1000,
                xaxis_title="Timestamp",
                yaxis_title="Value",
                legend_title="Crypto Symbol"
            )

            # Display chart in Streamlit
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"Metric '{selected_metric}' not found in the data.")
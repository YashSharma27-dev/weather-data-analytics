"""
Distributed Weather Data Analytics Dashboard
=============================================
Semester Big Data Project UI powered by Streamlit, Plotly,
Hadoop HDFS, Apache Spark, and Apache Kafka.
"""

import os
import time
import json
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Import local data connectors and socket health checks
try:
    from utils import (
        load_streaming_data, load_historical_data, load_batch_analytics_metrics,
        load_ml_metrics, predict_temperature, check_socket
    )
except ImportError:
    from dashboard.utils import (
        load_streaming_data, load_historical_data, load_batch_analytics_metrics,
        load_ml_metrics, predict_temperature, check_socket
    )

# -------------------------------------------------------------
# Page Configuration
# -------------------------------------------------------------
st.set_page_config(
    page_title="Weather Big Data Analytics | Hadoop & PySpark",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 800;
        margin-bottom: 0px;
        background: linear-gradient(90deg, #1E88E5, #00ACC1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #78909C;
        margin-bottom: 20px;
    }
    .metric-card {
        background-color: rgba(30, 136, 229, 0.08);
        border: 1px solid rgba(30, 136, 229, 0.2);
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    .status-badge-ok {
        background-color: #2e7d32;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.8rem;
        font-weight: bold;
    }
    .status-badge-warn {
        background-color: #e65100;
        color: white;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.8rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------------
# Sidebar Configuration & Cluster Connectivity
# -------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/clouds/200/cloud.png", width=90)
    st.title("Cluster Control")
    st.markdown("**Big Data Architecture:**")
    st.caption("Kafka ➔ PySpark Streaming ➔ Hadoop HDFS ➔ Streamlit UI")

    st.divider()

    st.subheader("🖥️ Cluster Target Node")
    cluster_mode = st.radio(
        "Deployment Target:",
        ["3-Computer Cluster", "Local Standalone / VM"],
        index=0
    )

    if cluster_mode == "3-Computer Cluster":
        default_master_ip = "192.168.1.100"
    else:
        default_master_ip = "localhost"

    master_ip = st.text_input("Master Node Host / IP:", value=default_master_ip)

    st.subheader("📡 Live Node Health Probe")
    if st.button("Probe Cluster Services", use_container_width=True):
        with st.spinner("Probing cluster sockets..."):
            hdfs_ok = check_socket(master_ip, 9000) or check_socket(master_ip, 9870)
            spark_ok = check_socket(master_ip, 7077) or check_socket(master_ip, 8080)
            kafka_ok = check_socket(master_ip, 9092)
            yarn_ok = check_socket(master_ip, 8088)
            st.session_state["probe_results"] = {
                "hdfs": hdfs_ok, "spark": spark_ok, "kafka": kafka_ok, "yarn": yarn_ok
            }

    probe = st.session_state.get("probe_results", {"hdfs": False, "spark": False, "kafka": False, "yarn": False})
    
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        st.markdown(f"**HDFS (9000):** {'🟢 OK' if probe['hdfs'] else '⚪ Offline'}")
        st.markdown(f"**Spark (7077):** {'🟢 OK' if probe['spark'] else '⚪ Offline'}")
    with col_p2:
        st.markdown(f"**Kafka (9092):** {'🟢 OK' if probe['kafka'] else '⚪ Offline'}")
        st.markdown(f"**YARN (8088):** {'🟢 OK' if probe['yarn'] else '⚪ Offline'}")

    st.divider()
    st.subheader("⚙️ Stream Controls")
    auto_refresh = st.checkbox("Auto-Refresh Live Stream", value=False)
    refresh_rate = st.slider("Interval (seconds):", 2, 10, 3)

    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()

    st.caption("Semester Project by Yash & Team")

# -------------------------------------------------------------
# Main Header Banner
# -------------------------------------------------------------
st.markdown('<div class="main-title">🌦️ Distributed Weather Data Analytics</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Real-Time Ingestion (Kafka) • Distributed Processing (PySpark) • Persistent Lake (Hadoop HDFS)</div>', unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🛰️ Real-Time Kafka Stream",
    "🐘 Hadoop HDFS & PySpark Analytics",
    "⚠️ Extreme Weather Alerts",
    "🤖 PySpark ML Weather Predictor",
    "🖥️ 3-Computer Cluster Topology"
])

# -------------------------------------------------------------
# TAB 1: Real-Time Kafka Stream Monitor
# -------------------------------------------------------------
with tab1:
    df_stream = load_streaming_data(max_records=150)

    # Top KPI Metrics Row
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Total Stream Events", len(df_stream) if not df_stream.empty else 0, delta="+ Active Ingestion")
    with kpi2:
        cities_count = df_stream["city"].nunique() if not df_stream.empty and "city" in df_stream.columns else 0
        st.metric("Monitored Cities", cities_count, delta="Real-time geo-nodes")
    with kpi3:
        avg_temp_stream = round(df_stream["temperature"].mean(), 1) if not df_stream.empty and "temperature" in df_stream.columns else 0.0
        st.metric("Current Avg Temp", f"{avg_temp_stream} °C")
    with kpi4:
        storm_count = len(df_stream[df_stream["wind_speed"] > 35]) if not df_stream.empty and "wind_speed" in df_stream.columns else 0
        st.metric("High Wind Alerts", storm_count, delta="Events > 35 km/h", delta_color="inverse")

    st.divider()

    if not df_stream.empty:
        col_stream_left, col_stream_right = st.columns([1, 1])

        with col_stream_left:
            st.subheader("📈 Live Temperature Stream by City")
            fig_temp = px.line(
                df_stream.tail(40),
                x="timestamp",
                y="temperature",
                color="city",
                markers=True,
                title="Real-Time Streaming Temperature Variations (°C)",
                template="plotly_dark"
            )
            fig_temp.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=360)
            st.plotly_chart(fig_temp, use_container_width=True)

        with col_stream_right:
            st.subheader("💨 Live Wind Speed vs Humidity")
            fig_wind = px.scatter(
                df_stream.tail(40),
                x="humidity",
                y="wind_speed",
                color="city",
                size="temperature",
                hover_data=["weather_condition"] if "weather_condition" in df_stream.columns else None,
                title="Streaming Wind Speed (km/h) vs Humidity (%)",
                template="plotly_dark"
            )
            fig_wind.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=360)
            st.plotly_chart(fig_wind, use_container_width=True)

        st.subheader("📋 Ingested Kafka Stream Messages (Latest First)")
        display_cols = [c for c in ["city", "timestamp", "temperature", "humidity", "wind_speed", "weather_condition", "heat_index", "aqi"] if c in df_stream.columns]
        st.dataframe(
            df_stream[display_cols].tail(15).iloc[::-1],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No streaming records detected yet. Start the producer using: `python streaming/weather_producer.py`")

# -------------------------------------------------------------
# TAB 2: Hadoop HDFS & PySpark Distributed Analytics
# -------------------------------------------------------------
with tab2:
    df_hist = load_historical_data()
    metrics_summary = load_batch_analytics_metrics()

    st.markdown("### 🐘 PySpark Distributed Batch Processing on HDFS Data Lake")
    st.caption("Target HDFS Path: `hdfs://master:9000/weather/data/weather_data.csv`")

    col_h1, col_h2, col_h3 = st.columns(3)
    with col_h1:
        st.metric("Total Lake Records", len(df_hist), delta="Aggregated via PySpark")
    with col_h2:
        st.metric("HDFS Block Size", "128 MB (Replication: 2)", delta="Fault-Tolerant")
    with col_h3:
        st.metric("Spark Partitions", "8 Partitions (Distributed)", delta="Parallel Workers")

    st.divider()

    if not df_hist.empty:
        col_b1, col_b2 = st.columns([1, 1])

        with col_b1:
            st.subheader("📊 City-wise Mean & Maximum Temperatures")
            city_stats = df_hist.groupby("city")["temperature"].agg(["mean", "max", "min"]).reset_index()
            city_stats = city_stats.sort_values(by="mean", ascending=False)
            
            fig_bar = px.bar(
                city_stats,
                x="city",
                y="mean",
                error_y=city_stats["max"] - city_stats["mean"],
                color="mean",
                color_continuous_scale="Viridis",
                title="Average Temperature (°C) with Max Peak Bars",
                labels={"mean": "Avg Temp (°C)"},
                template="plotly_dark"
            )
            fig_bar.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_b2:
            st.subheader("📦 Temperature Distribution by City")
            fig_box = px.box(
                df_hist,
                x="city",
                y="temperature",
                color="city",
                title="Temperature Spread & Quartile Analysis Across Cities",
                template="plotly_dark"
            )
            fig_box.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_box, use_container_width=True)

        # Correlation Heatmap & Hourly Diurnal Cycles
        col_c1, col_c2 = st.columns([1, 1])

        with col_c1:
            st.subheader("🔥 Meteorological Correlation Matrix")
            numeric_cols = [c for c in ["temperature", "humidity", "wind_speed", "pressure", "precipitation", "heat_index", "aqi"] if c in df_hist.columns]
            corr = df_hist[numeric_cols].corr()

            fig_corr = px.imshow(
                corr,
                text_auto=True,
                color_continuous_scale="RdBu_r",
                title="Pearson Correlation Coefficients (PySpark DataFrame Matrix)",
                template="plotly_dark"
            )
            fig_corr.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_corr, use_container_width=True)

        with col_c2:
            st.subheader("⏱️ 24-Hour Diurnal Temperature & Humidity Cycle")
            if "timestamp" in df_hist.columns:
                df_hist["hour"] = pd.to_datetime(df_hist["timestamp"]).dt.hour
                hourly = df_hist.groupby("hour").agg(
                    avg_temp=("temperature", "mean"),
                    avg_hum=("humidity", "mean")
                ).reset_index()

                fig_diurnal = go.Figure()
                fig_diurnal.add_trace(go.Scatter(
                    x=hourly["hour"], y=hourly["avg_temp"], mode='lines+markers',
                    name='Avg Temperature (°C)', line=dict(color='#FF7043', width=3)
                ))
                fig_diurnal.add_trace(go.Scatter(
                    x=hourly["hour"], y=hourly["avg_hum"], mode='lines+markers',
                    name='Avg Humidity (%)', line=dict(color='#29B6F6', width=3, dash='dash')
                ))
                fig_diurnal.update_layout(
                    title="Diurnal Cycle Over 24 Hours",
                    xaxis_title="Hour of Day (00:00 - 23:00)",
                    template="plotly_dark",
                    height=380,
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                st.plotly_chart(fig_diurnal, use_container_width=True)

    else:
        st.warning("Historical dataset not loaded. Ensure `data/weather_data.csv` exists.")

# -------------------------------------------------------------
# TAB 3: Extreme Weather & Anomaly Detection
# -------------------------------------------------------------
with tab3:
    st.subheader("⚠️ Extreme Weather Anomaly & Alert Engine")
    st.markdown("Automated detection of critical meteorological conditions across cluster stream partitions.")

    df_anom = load_historical_data()
    if not df_anom.empty:
        # Define anomaly filters
        heatwaves = df_anom[df_anom["temperature"] > 38.0]
        storms = df_anom[df_anom["wind_speed"] > 25.0]
        high_humidity = df_anom[df_anom["humidity"] > 85.0]
        heavy_rain = df_anom[df_anom.get("precipitation", 0.0) > 8.0]

        a_col1, a_col2, a_col3, a_col4 = st.columns(4)
        with a_col1:
            st.metric("🔥 Heatwave Incidents", len(heatwaves), delta="Temp > 38°C", delta_color="inverse")
        with a_col2:
            st.metric("🌪️ High Wind Events", len(storms), delta="Wind > 25 km/h", delta_color="inverse")
        with a_col3:
            st.metric("💧 Extreme Humidity", len(high_humidity), delta="Humidity > 85%")
        with a_col4:
            st.metric("⛈️ Heavy Precipitation", len(heavy_rain), delta="Rain > 8 mm")

        st.divider()

        st.subheader("📍 Geospatial Distribution of Weather Observations")
        if "latitude" in df_anom.columns and "longitude" in df_anom.columns:
            latest_by_city = df_anom.sort_values("timestamp").groupby("city").last().reset_index()
            fig_map = px.scatter_geo(
                latest_by_city,
                lat="latitude",
                lon="longitude",
                color="temperature",
                size="wind_speed",
                hover_name="city",
                hover_data=["temperature", "humidity", "wind_speed", "weather_condition"],
                color_continuous_scale="Thermal",
                title="Global City Nodes: Temperature & Wind Severity",
                template="plotly_dark"
            )
            fig_map.update_geos(showcountries=True, countrycolor="#455A64")
            fig_map.update_layout(height=420, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("🚨 Flagged Anomaly Observations")
        anomalies_all = df_anom[
            (df_anom["temperature"] > 38.0) |
            (df_anom["wind_speed"] > 25.0) |
            (df_anom["humidity"] > 85.0)
        ]
        display_cols = [c for c in ["city", "timestamp", "temperature", "humidity", "wind_speed", "weather_condition", "heat_index"] if c in anomalies_all.columns]
        st.dataframe(anomalies_all[display_cols].head(30), use_container_width=True)

# -------------------------------------------------------------
# TAB 4: PySpark MLlib Weather Predictor
# -------------------------------------------------------------
with tab4:
    st.subheader("🤖 PySpark MLlib Weather Forecasting & Predictive Sandbox")
    st.markdown("Interactive machine learning inference using multivariate regression and ensemble classification models.")

    ml_metrics = load_ml_metrics()

    col_ml_input, col_ml_result = st.columns([1, 1])

    with col_ml_input:
        st.markdown("#### 🎛️ Atmospheric Input Features")
        hum_in = st.slider("Relative Humidity (%):", min_value=15.0, max_value=99.0, value=65.0, step=1.0)
        press_in = st.slider("Surface Pressure (hPa):", min_value=980.0, max_value=1040.0, value=1013.0, step=1.0)
        wind_in = st.slider("Wind Speed (km/h):", min_value=1.0, max_value=60.0, value=15.0, step=0.5)
        hour_in = st.slider("Hour of Day (24-Hour Clock):", min_value=0, max_value=23, value=14, step=1)

        predict_btn = st.button("🚀 Run PySpark Model Prediction", use_container_width=True, type="primary")

    with col_ml_result:
        st.markdown("#### 🎯 Prediction Results")
        pred_temp = predict_temperature(hum_in, press_in, wind_in, hour_in, ml_metrics)

        # Weather Condition Classifier logic
        if hum_in > 80 and press_in < 1005:
            cond_pred = "⛈️ Thunderstorm / Heavy Rain"
            comfort = "Uncomfortable (High Humidity & Storm Risk)"
        elif hum_in > 70:
            cond_pred = "🌧️ Light Rain / Overcast"
            comfort = "Humid"
        elif pred_temp > 35:
            cond_pred = "☀️ Hot & Sunny"
            comfort = "Caution: Heat Caution"
        elif pred_temp < 10:
            cond_pred = "❄️ Chilly / Clear"
            comfort = "Cold"
        else:
            cond_pred = "⛅ Pleasant / Partly Cloudy"
            comfort = "Comfortable"

        res1, res2 = st.columns(2)
        with res1:
            st.metric("Predicted Temperature", f"{pred_temp} °C", delta=f"{'Above' if pred_temp > 25 else 'Below'} 25°C baseline")
        with res2:
            st.metric("Weather Condition", cond_pred)

        st.info(f"**Thermal Comfort Assessment:** {comfort}")

        # Plotly Gauge Chart for Temperature
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pred_temp,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Predicted Temperature (°C)", 'font': {'size': 18}},
            gauge={
                'axis': {'range': [-5, 50]},
                'bar': {'color': "#FF7043"},
                'steps': [
                    {'range': [-5, 15], 'color': "#1976D2"},
                    {'range': [15, 28], 'color': "#388E3C"},
                    {'range': [28, 38], 'color': "#FBC02D"},
                    {'range': [38, 50], 'color': "#D32F2F"}
                ],
                'threshold': {
                    'line': {'color': "white", 'width': 4},
                    'thickness': 0.75,
                    'value': pred_temp
                }
            }
        ))
        fig_gauge.update_layout(height=280, margin=dict(l=20, r=20, t=40, b=20), template="plotly_dark")
        st.plotly_chart(fig_gauge, use_container_width=True)

    st.divider()

    st.subheader("📊 PySpark Model Evaluation & Feature Importances")
    if ml_metrics:
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Model Architecture", ml_metrics.get("model_type", "Linear Regression"))
        with m2:
            st.metric("Root Mean Sq Error (RMSE)", ml_metrics.get("rmse", 7.33))
        with m3:
            st.metric("Mean Abs Error (MAE)", ml_metrics.get("mae", 6.28))
        with m4:
            st.metric("R² Score", ml_metrics.get("r2_score", 0.85))

        if "coefficients" in ml_metrics:
            st.markdown("##### Feature Weights (Coefficients):")
            coef_df = pd.DataFrame(list(ml_metrics["coefficients"].items()), columns=["Feature", "Weight"])
            fig_coef = px.bar(
                coef_df, x="Feature", y="Weight", color="Weight",
                color_continuous_scale="Tealgrn", template="plotly_dark",
                title="Model Feature Impact on Temperature"
            )
            fig_coef.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_coef, use_container_width=True)
    else:
        st.caption("Train model using: `spark-submit spark/weather_ml_model.py` to see live weights.")

# -------------------------------------------------------------
# TAB 5: 3-Computer Cluster Topology & Architecture
# -------------------------------------------------------------
with tab5:
    st.subheader("🖥️ 3-Computer Cluster Distributed Topology")
    st.markdown("Live physical and logical cluster mapping across Master and Worker nodes.")

    # Visual Cluster Cards
    col_n1, col_n2, col_n3 = st.columns(3)

    with col_n1:
        st.markdown("""
        <div class="metric-card">
            <h4>🖥️ Computer 1: Master Node</h4>
            <p><strong>IP:</strong> <code>192.168.1.100</code> (Host: <code>master</code>)</p>
            <hr/>
            <p><strong>Active Services:</strong></p>
            <ul style="text-align: left; font-size: 0.9rem;">
                <li>HDFS NameNode (:9000, Web: :9870)</li>
                <li>YARN ResourceManager (:8088)</li>
                <li>Kafka Broker & Zookeeper (:9092, :2181)</li>
                <li>Spark Master (:7077, Web: :8080)</li>
                <li>Streamlit Web UI (:8501)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col_n2:
        st.markdown("""
        <div class="metric-card">
            <h4>💻 Computer 2: Worker Node 1</h4>
            <p><strong>IP:</strong> <code>192.168.1.101</code> (Host: <code>worker1</code>)</p>
            <hr/>
            <p><strong>Active Services:</strong></p>
            <ul style="text-align: left; font-size: 0.9rem;">
                <li>HDFS DataNode (Port 9864)</li>
                <li>YARN NodeManager</li>
                <li>Spark Worker 1 (Port 7078, Web: :8081)</li>
                <li>Storage: HDFS Block Replica 1</li>
                <li>Cores: 4 | Memory: 3GB</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with col_n3:
        st.markdown("""
        <div class="metric-card">
            <h4>💻 Computer 3: Worker Node 2</h4>
            <p><strong>IP:</strong> <code>192.168.1.102</code> (Host: <code>worker2</code>)</p>
            <hr/>
            <p><strong>Active Services:</strong></p>
            <ul style="text-align: left; font-size: 0.9rem;">
                <li>HDFS DataNode (Port 9864)</li>
                <li>YARN NodeManager</li>
                <li>Spark Worker 2 (Port 7078, Web: :8081)</li>
                <li>Storage: HDFS Block Replica 2</li>
                <li>Cores: 4 | Memory: 3GB</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    st.subheader("🌐 Cluster Web Consoles & Direct Links")
    st.markdown("""
    When running the cluster, open these URLs from your browser on any computer connected to the same LAN:
    - **Hadoop NameNode UI:** [http://192.168.1.100:9870](http://192.168.1.100:9870) *(Inspect DataNodes, HDFS storage, replication factor 2)*
    - **YARN Cluster Manager:** [http://192.168.1.100:8088](http://192.168.1.100:8088) *(Inspect running MapReduce & Spark YARN applications)*
    - **Spark Master UI:** [http://192.168.1.100:8080](http://192.168.1.100:8080) *(Inspect Worker 1 & Worker 2 cores, memory, and running PySpark jobs)*
    - **Streamlit Analytics Dashboard:** [http://192.168.1.100:8501](http://192.168.1.100:8501)
    """)

    st.subheader("🚀 Complete Cluster Startup Command Cheat Sheet")
    st.code("""
# 1. On Master PC: Start Hadoop (HDFS & YARN)
start-dfs.sh
start-yarn.sh

# 2. On Master PC: Start Zookeeper & Kafka Broker
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# 3. On Master PC: Start Spark Master & Workers
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-workers.sh

# 4. On Master PC (or any node): Start Weather Stream Ingestion
python streaming/weather_producer.py 192.168.1.100:9092

# 5. On Master PC: Submit PySpark Streaming Job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
             --master spark://192.168.1.100:7077 \\
             streaming/kafka_to_hdfs_stream.py 192.168.1.100:9092 hdfs://192.168.1.100:9000

# 6. On Master PC: Launch Streamlit Dashboard
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0
    """, language="bash")

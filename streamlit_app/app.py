import streamlit as st
import pandas as pd
import math
import os
from cassandra.cluster import Cluster

st.set_page_config(
    page_title="Taxi Analytics Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Modern Slick CSS
st.markdown("""
<style>
    /* Global Styles */
    .main {
        background-color: #0f1116;
    }
    
    .main-header {
        font-size: 2.8rem;
        font-weight: 800;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
        font-family: 'Inter', sans-serif;
    }
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(145deg, #1e2229, #2a303c);
        padding: 1.8rem 1.5rem;
        border-radius: 16px;
        border: 1px solid #2a303c;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    
    .metric-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #667eea, #764ba2);
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(102, 126, 234, 0.2);
        border-color: #667eea;
    }
    
    /* Status Indicators */
    .status-connected {
        color: #00d4aa;
        font-weight: 700;
        padding: 0.5rem 1rem;
        background: rgba(0, 212, 170, 0.1);
        border-radius: 20px;
        border: 1px solid rgba(0, 212, 170, 0.3);
        display: inline-block;
    }
    
    .status-disconnected {
        color: #ff6b6b;
        font-weight: 700;
        padding: 0.5rem 1rem;
        background: rgba(255, 107, 107, 0.1);
        border-radius: 20px;
        border: 1px solid rgba(255, 107, 107, 0.3);
        display: inline-block;
    }
    
    /* Section Headers */
    .section-header {
        font-size: 1.6rem;
        font-weight: 700;
        color: #ffffff;
        margin: 2rem 0 1.5rem 0;
        padding-bottom: 0.8rem;
        border-bottom: 2px solid;
        border-image: linear-gradient(90deg, #667eea, #764ba2) 1;
        font-family: 'Inter', sans-serif;
    }
    
    /* Sidebar Styling */
    .css-1d391kg, .css-1lcbmhc {
        background: linear-gradient(180deg, #1a1d23 0%, #0f1116 100%);
        border-right: 1px solid #2a303c;
    }
    
    /* Button Styling */
    .stButton button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.8rem 1.5rem;
        border-radius: 12px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #1a1d23;
        padding: 8px;
        border-radius: 12px;
        margin-bottom: 2rem;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        padding: 0.8rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
        border: 1px solid transparent;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: 1px solid #667eea;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }
    
    .stTabs [aria-selected="false"] {
        background: #2a303c;
        color: #8892b0;
        border: 1px solid #2a303c;
    }
    
    /* Dataframe Styling */
    .dataframe {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #2a303c;
    }
    
    /* Input Fields */
    .stNumberInput input, .stTextInput input {
        background: #1e2229 !important;
        border: 1px solid #2a303c !important;
        border-radius: 10px !important;
        color: white !important;
        padding: 0.8rem !important;
    }
    
    .stNumberInput input:focus, .stTextInput input:focus {
        border-color: #667eea !important;
        box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.2) !important;
    }
    
    /* Slider Styling */
    .stSlider {
        margin-top: 1rem;
    }
    
    .stSlider > div > div {
        background: linear-gradient(90deg, #667eea, #764ba2);
    }
    
    /* Success/Info/Warning Messages */
    .stSuccess {
        background: rgba(0, 212, 170, 0.1);
        border: 1px solid rgba(0, 212, 170, 0.3);
        border-radius: 12px;
        padding: 1rem;
    }
    
    .stInfo {
        background: rgba(102, 126, 234, 0.1);
        border: 1px solid rgba(102, 126, 234, 0.3);
        border-radius: 12px;
        padding: 1rem;
    }
    
    .stWarning {
        background: rgba(255, 193, 7, 0.1);
        border: 1px solid rgba(255, 193, 7, 0.3);
        border-radius: 12px;
        padding: 1rem;
    }
    
    .stError {
        background: rgba(255, 107, 107, 0.1);
        border: 1px solid rgba(255, 107, 107, 0.3);
        border-radius: 12px;
        padding: 1rem;
    }
    
    /* Container Styling */
    .css-1r6slb0 {
        background: #1a1d23;
        border: 1px solid #2a303c;
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
    }
    
    /* Footer */
    footer {
        color: #8892b0 !important;
        font-size: 0.9rem;
    }
    
    /* Map Container */
    .stDeckGlJsonChart {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #2a303c;
    }
    
    /* Spinner Animation */
    .stSpinner > div {
        border-color: #667eea transparent transparent transparent !important;
    }
    
    /* Metric Value Emphasis */
    [data-testid="metric-container"] {
        background: linear-gradient(145deg, #1e2229, #2a303c);
        border: 1px solid #2a303c;
        border-radius: 16px;
        padding: 1.5rem;
    }
    
    [data-testid="metric-container"] > div > label {
        color: #8892b0 !important;
        font-weight: 600;
    }
    
    [data-testid="metric-container"] > div > div {
        color: #ffffff !important;
        font-weight: 800;
        font-size: 1.8rem;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #1a1d23;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #667eea;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #764ba2;
    }
    
    /* Fix for dataframe stuttering */
    .stDataFrame {
        border: 1px solid #2a303c;
        border-radius: 12px;
    }
    
    /* Ensure smooth rendering */
    .stDataFrame > div > div {
        border-radius: 12px;
    }
</style>
""", unsafe_allow_html=True)

def test_cassandra_connection():
    try:
        cluster = Cluster([os.getenv("CASSANDRA_HOST", "cassandra")], port=9042)
        session = cluster.connect()
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'taxi_tracking'")
        if rows.one():
            session.shutdown()
            cluster.shutdown()
            return True, "Operational"
        else:
            session.shutdown()
            cluster.shutdown()
            return False, "Keyspace Missing"
    except Exception as e:
        return False, f"Connection Error: {str(e)}"

def get_table_data_pandas(table_name, keyspace="taxi_tracking", limit=100):
    try:
        cluster = Cluster([os.getenv("CASSANDRA_HOST", "cassandra")], port=9042)
        session = cluster.connect(keyspace)
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        rows = session.execute(query)
        data = [dict(row._asdict()) for row in rows]
        session.shutdown()
        cluster.shutdown()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading {table_name}: {str(e)}")
        return pd.DataFrame()

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def find_nearest_taxis(lat, lon, n=5):
    try:
        df = get_table_data_pandas("positions")
        if df.empty:
            return pd.DataFrame()
        available_taxis = df[df['available'] == True]
        if available_taxis.empty:
            return pd.DataFrame()
        distances = []
        for _, taxi in available_taxis.iterrows():
            dist = haversine_distance(lat, lon, taxi['latitude'], taxi['longitude'])
            taxi_data = taxi.to_dict()
            taxi_data['distance_km'] = round(dist, 2)
            distances.append(taxi_data)
        nearest = sorted(distances, key=lambda x: x['distance_km'])[:n]
        return pd.DataFrame(nearest)
    except Exception as e:
        st.error(f"Error finding taxis: {e}")
        return pd.DataFrame()

def estimate_trip_cost(lat1, lon1, lat2, lon2):
    try:
        dist = haversine_distance(lat1, lon1, lat2, lon2)
        df_trips = get_table_data_pandas("trips")
        if df_trips.empty:
            return round(dist, 2), None
        valid_trips = df_trips[df_trips['trip_distance'] > 0]
        if valid_trips.empty:
            return round(dist, 2), None
        valid_trips['cost_per_km'] = valid_trips['amount'] / valid_trips['trip_distance']
        avg_cost_per_km = valid_trips['cost_per_km'].mean()
        estimated_cost = round(dist * avg_cost_per_km, 2) if not pd.isna(avg_cost_per_km) else None
        return round(dist, 2), estimated_cost
    except Exception as e:
        st.error(f"Error estimating cost: {e}")
        return None, None

# Header Section
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown('<div class="main-header"> Taxi Analytics Dashboard</div>', unsafe_allow_html=True)
    st.markdown("### Real-time operational intelligence and performance metrics")
    st.markdown("*Smart fleet management powered by real-time data analytics*")
with col2:
    st.markdown("")
    connected, message = test_cassandra_connection()
    status_color = "status-connected" if connected else "status-disconnected"
    st.markdown(f'<div class="{status_color}">{message}</div>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("###  System Control Panel")
    
    if st.button(" Refresh Data", use_container_width=True, type="primary"):
        st.rerun()
    
    st.markdown("---")
    st.markdown("###  Data Overview")
    
    df_positions = get_table_data_pandas("positions")
    df_trips = get_table_data_pandas("trips")
    
    st.metric("Active Vehicles", len(df_positions) if not df_positions.empty else 0)
    st.metric("Total Trips", len(df_trips) if not df_trips.empty else 0)
    
    if not df_positions.empty:
        available_count = len(df_positions[df_positions['available'] == True])
        utilization_rate = (available_count / len(df_positions)) * 100 if len(df_positions) > 0 else 0
        st.metric("Fleet Utilization", f"{utilization_rate:.1f}%")
    
    st.markdown("---")
    st.markdown("####  About")
    st.markdown("""
    Real-time analytics platform for taxi fleet management and operational intelligence.
    
    **Features:**
    - Live vehicle tracking
    - Trip analytics
    - Cost estimation
    - Performance metrics
    """)

# Main Content Tabs
tab1, tab2, tab3 = st.tabs(["Operational Dashboard", " Fleet Locator", " Business Intelligence"])

with tab1:
    st.markdown('<div class="section-header"> Fleet Overview</div>', unsafe_allow_html=True)
    
    # Key Metrics - Removed div containers
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_taxis = len(df_positions) if not df_positions.empty else 0
        st.metric("Total Fleet", total_taxis)
    
    with col2:
        available_count = len(df_positions[df_positions['available'] == True]) if not df_positions.empty else 0
        st.metric("Available Now", available_count)
    
    with col3:
        trips_count = len(df_trips) if not df_trips.empty else 0
        st.metric("Completed Trips", trips_count)
    
    with col4:
        revenue = df_trips['amount'].sum() if not df_trips.empty else 0
        st.metric("Total Revenue", f"${revenue:,.0f}")
    
    # Data Preview Section
    st.markdown('<div class="section-header"> Live Data Feed</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("** Vehicle Positions**")
        if not df_positions.empty:
            display_positions = df_positions[['taxi_id', 'latitude', 'longitude', 'available', 'timestamp']].head(8)
            st.dataframe(display_positions, use_container_width=True, height=300)
        else:
            st.info(" No vehicle data available")
    
    with col2:
        st.markdown("** Recent Trips**")
        if not df_trips.empty:
            display_trips = df_trips[['trip_id', 'trip_distance', 'amount', 'start_time']].head(8)
            st.dataframe(display_trips, use_container_width=True, height=300)
        else:
            st.info(" No trip data available")

with tab2:
    st.markdown('<div class="section-header"> Fleet Location Services</div>', unsafe_allow_html=True)
    
    # Input Section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("** Search Parameters**")
        with st.container():
            col1a, col1b = st.columns(2)
            with col1a:
                lat = st.number_input("Latitude", value=40.75, format="%.6f", help="Enter your current latitude")
            with col1b:
                lon = st.number_input("Longitude", value=-73.98, format="%.6f", help="Enter your current longitude")
            
            n_taxis = st.slider("Number of vehicles to display", 1, 10, 5)
    
    with col2:
        st.markdown("** Actions**")
        st.markdown("")
        locate_clicked = st.button(" Locate Nearest Vehicles", type="primary", use_container_width=True)
    
    # Results Section - Full width below inputs
    if locate_clicked:
        with st.spinner("🛰️ Scanning fleet locations..."):
            result = find_nearest_taxis(lat, lon, n_taxis)
            
            if not result.empty:
                st.success(f"**✅ {len(result)} vehicles found near your location**")
                
                # Create two columns for results display
                col_results1, col_results2 = st.columns([1, 1])
                
                with col_results1:
                    st.markdown("** Nearest Available Vehicles**")
                    display_cols = ['taxi_id', 'distance_km', 'latitude', 'longitude']
                    
                    # Use a simple dataframe display without additional formatting
                    st.dataframe(
                        result[display_cols], 
                        use_container_width=True,
                        hide_index=True
                    )
                
                with col_results2:
                    st.markdown("** Geographical View**")
                    # Prepare data for map
                    map_data = result[["latitude", "longitude"]].copy()
                    map_data.columns = ['lat', 'lon']
                    st.map(map_data, use_container_width=True)
                    
                # Additional metrics in a separate row
                st.markdown("** Quick Stats**")
                col_metrics1, col_metrics2, col_metrics3, col_metrics4 = st.columns(4)
                
                with col_metrics1:
                    min_distance = result['distance_km'].min()
                    st.metric("Closest Vehicle", f"{min_distance} km")
                
                with col_metrics2:
                    max_distance = result['distance_km'].max()
                    st.metric("Furthest Vehicle", f"{max_distance} km")
                
                with col_metrics3:
                    avg_distance = result['distance_km'].mean()
                    st.metric("Average Distance", f"{avg_distance:.1f} km")
                
                with col_metrics4:
                    st.metric("Response Time", "Real-time")
                    
            else:
                st.warning("""
                ** No available vehicles found**
                
                This could be because:
                - All vehicles are currently occupied
                - No vehicles in the specified area
                - Connection issues with the database
                
                Try adjusting your location or check back later.
                """)

with tab3:
    st.markdown('<div class="section-header">💼 Business Intelligence</div>', unsafe_allow_html=True)
    
    tab3a, tab3b = st.tabs([" Performance Analytics", " Cost Estimation"])
    
    with tab3a:
        if not df_trips.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                avg_fare = df_trips['amount'].mean()
                st.metric("Average Fare", f"${avg_fare:.2f}")
            
            with col2:
                avg_distance = df_trips['trip_distance'].mean()
                st.metric("Average Distance", f"{avg_distance:.1f} km")
            
            with col3:
                total_revenue = df_trips['amount'].sum()
                st.metric("Total Revenue", f"${total_revenue:,.0f}")
            
            with col4:
                total_distance = df_trips['trip_distance'].sum()
                st.metric("Total Distance", f"{total_distance:,.1f} km")
            
            st.markdown("** Detailed Trip History**")
            st.dataframe(df_trips, use_container_width=True, height=400)
        else:
            st.info(" No trip data available for analysis")
    
    with tab3b:
        st.markdown("**Trip Cost Calculator**")
        
        with st.container():
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("** Origin**")
                pickup_lat = st.number_input("Latitude", value=40.75, format="%.6f", key="pickup_lat")
                pickup_lon = st.number_input("Longitude", value=-73.98, format="%.6f", key="pickup_lon")
            
            with col2:
                st.markdown("** Destination**")
                drop_lat = st.number_input("Latitude", value=40.78, format="%.6f", key="drop_lat")
                drop_lon = st.number_input("Longitude", value=-73.96, format="%.6f", key="drop_lon")
        
        if st.button(" Calculate Estimate", type="primary"):
            with st.spinner("Analyzing historical data..."):
                dist, cost = estimate_trip_cost(pickup_lat, pickup_lon, drop_lat, drop_lon)
                
                if dist is not None:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Estimated Distance", f"{dist} km")
                    with col2:
                        if cost:
                            st.metric("Estimated Fare", f"${cost}")
                        else:
                            st.info("Insufficient data for accurate estimation")
                else:
                    st.error(" Unable to calculate distance")

# Footer
st.markdown("---")
col1, col2 = st.columns([2, 3])
with col1:
    st.markdown("** Taxi Analytics Platform** • Real-time Fleet Management System")
    st.markdown("*Built with Streamlit & Cassandra*")
with col2:
    st.markdown("**Developed by:** Iyed Mekki • Mohamed Adem Arfaoui • Ranim Manai")
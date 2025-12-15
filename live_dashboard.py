import time
import random
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

STREAM_ID = "nflx_live_event_superbowl_v1"
SEGMENT_DURATION = 2.0  
AD_BREAK_DURATION = 30.0 
CHAOS_MODE = True 

st.set_page_config(page_title="Netflix Ads Analytics", layout="wide")
st.title("Netflix Ads: Real-Time QoE & Revenue Monitor")

# sidebar info
with st.sidebar:
    st.header("Project Context: Netflix Ads")
    
    st.info("""
    **Role:** Analytics Engineer
    **Goal:** Translate upstream *Engineering Signals* into downstream *Business Impact*.
    """)
    
    st.divider()
    
    st.subheader("1. The Upstream Signal (Cause)")
    st.markdown("""
    **The SCTE-35 Trigger**
    * **What it is:** A hex code (e.g., `0xFC30...`) in the HLS manifest.
    * **Function:** Tells the video player: *"Stop the movie, splice in an ad."*
    * **The Failure:** The trigger fires, but the payload is `NULL`.
    """)
    
    st.subheader("2. The Downstream Impact (Effect)")
    st.error("""
    **Business Consequence**
    * **Technical Result:** "Dead Air" (Black Screen) because the server doesn't know which ad to play.
    * **Business Result:** **0 Impressions**.
    * **Financial Impact:** **$0 Revenue** generated for this ad slot.
    """)

    st.subheader("3. Quality of Experience (QoE)")
    st.warning("""
    **Latency vs. Churn**
    * **Logic:** If `latency > 1s` during an ad, viewers rage-quit.
    * **Metric:** MODELED CHURN (See Dashboard).
    """)
    
    st.caption("Simulation Running...")

#setup
tab1, tab2 = st.tabs(["Business Impact Dashboard", "Data Lineage & SQL"])

#tab1
with tab1:
    metric_col1, metric_col2, metric_col3 = st.columns(3)
    
    with metric_col1:
        viewer_metric = st.empty()
    with metric_col2:
        revenue_metric = st.empty()
    with metric_col3:
        health_metric = st.empty()
        
    st.divider()
    chart_placeholder = st.empty()
    alert_placeholder = st.empty()

#tab2
with tab2:
    st.markdown("### dbt / SQL Transformation Layer")
    st.markdown("*As an Analytics Engineer, I would model this raw telemetry into a business-ready table.*")
    st.code("""
    -- dbt modeling: mart_ads_quality_experience
    WITH raw_telemetry AS (
        SELECT stream_id, timestamp, event_type, latency_ms, scte35_payload
        FROM {{ source('telemetry', 'stream_logs') }}
    ),
    flagged_events AS (
        SELECT *,
            --if an ad is playing and latency is high then that means there's buffering happening
            CASE WHEN event_type = 'ad_playing' AND latency_ms > 1000 THEN 1 ELSE 0 END AS is_ad_buffering_event,
            
            -- if there is an ad trigger but no payload then there is a signal failure and screen is black
            CASE WHEN event_type = 'scte35_trigger' AND scte35_payload IS NULL THEN 1 ELSE 0 END AS is_signal_failure
        FROM raw_telemetry
    )
    SELECT 
        stream_id,
        DATE_TRUNC('hour', timestamp) as hour_bucket,
        COUNT(*) as total_packets,
        SUM(is_ad_buffering_event) as total_buffering_events,
        SUM(is_signal_failure) as failed_ad_insertions
    FROM flagged_events
    GROUP BY 1, 2
    """, language='sql')

#global values
if 'df2' not in st.session_state:
    st.session_state.df2 = pd.DataFrame()
if 'viewers' not in st.session_state:
    st.session_state.viewers = 10000  
if 'revenue_lost' not in st.session_state:
    st.session_state.revenue_lost = 0.0

def generate_scte35_payload_mock():
    return "0xFC30" + "".join([random.choice("0123456789ABCDEF") for _ in range(8)])

#loop for raw data generation
def generate_stream_data():
    sequence_number = 0
    in_ad_break = False
    ad_break_remaining = 0
    
    while True:
        current_time = datetime.now(timezone.utc).isoformat()
        scte35_payload = None
        event_type = "content_playing"
        
        #raw events logic
        if in_ad_break:
            segment_type = "ad"
            event_type = "ad_playing"
            ad_break_remaining -= SEGMENT_DURATION
            if ad_break_remaining <= 0:
                in_ad_break = False
                event_type = "ad_complete"
        else:
            segment_type = "content"
            event_type = "content_playing"
            if random.random() < 0.10: 
                in_ad_break = True
                ad_break_remaining = AD_BREAK_DURATION
                event_type = "scte35_trigger"
                scte35_payload = generate_scte35_payload_mock()
                if CHAOS_MODE and random.random() < 0.20:
                    scte35_payload = None 

        latency_ms = random.gauss(200, 20)
        if CHAOS_MODE and segment_type == "ad" and random.random() < 0.30:
            latency_ms += 2000 

        data_packet = {
            "stream_id": STREAM_ID,
            "timestamp": current_time,
            "seq_id": sequence_number,
            "event": event_type,
            "scte35_payload": scte35_payload if scte35_payload else "null",
            "latency_ms": round(latency_ms, 2)
        }

        #processing each line as it comes in
        if len(st.session_state.df2) >= 50:
            st.session_state.df2 = st.session_state.df2.iloc[1:]
            
        df = pd.DataFrame([data_packet])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        is_buffering = (df['latency_ms'] >= 1000) & (df['event'] == 'ad_playing')
        df['ad_buffering'] = is_buffering
        
        #logic to showcase that if members drop than that impacts views and rev
        if is_buffering.any():
            drop_off = random.randint(50, 150)
            st.session_state.viewers -= drop_off
            st.session_state.revenue_lost += (drop_off * 0.05) 
        else:
            #forcing this to be slower 
            if st.session_state.viewers < 10000:
                st.session_state.viewers += random.randint(5, 20)

        df['time_only'] = df['timestamp'].dt.strftime('%H:%M:%S')
        st.session_state.df2 = pd.concat([st.session_state.df2, df], ignore_index=True)

        #biz metrics
        viewer_metric.metric("Live Viewers", f"{st.session_state.viewers:,}")
        revenue_metric.metric("Revenue at Risk", f"${st.session_state.revenue_lost:,.2f}", delta_color="inverse")
        
        if "ad" in event_type:
             health_metric.warning("Ad Break Active")
        else:
             health_metric.success("Content Playing")

        chart_placeholder.line_chart(st.session_state.df2, x='time_only', y='latency_ms')
        
        if is_buffering.any():
            alert_placeholder.error(f"CHURN RISK: Ad Buffering. {drop_off} viewers exited.")
        elif st.session_state.viewers < 9000:
             alert_placeholder.warning("Viewer count degrading due to poor ad experience.")
        else:
            alert_placeholder.success("Ad Experience: Optimal")

        sequence_number += 1
        time.sleep(SEGMENT_DURATION)

if __name__ == "__main__":
    generate_stream_data()

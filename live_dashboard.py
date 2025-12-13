import time
import random
import pandas as pd
import streamlit as st
from datetime import datetime, timezone

# --- CONFIGURATION ---
STREAM_ID = "nflx_live_event_superbowl_v1"
SEGMENT_DURATION = 2.0  
AD_BREAK_DURATION = 30.0 
CHAOS_MODE = True 

# Setup the Streamlit Page
st.title("Netflix Ads Live Monitor")
st.write("Monitoring Stream: " + STREAM_ID)

# --- 1. SETUP CHART (OUTSIDE THE LOOP) ---
# We create the placeholder ONCE so we can reuse it
chart_placeholder = st.empty()
# We also create a separate placeholder for text alerts below the chart
alert_placeholder = st.empty()


# --- SIDEBAR: PROJECT CONTEXT & EXPLANATIONS ---
with st.sidebar:
    st.header("Project Context: Netflix Ads Engineer")
    st.markdown("""
    **Goal:** Simulate the "Health of Ad Operations" monitoring pipeline.
    
    **Technical Foundation:** Based on Netflix Patent *12262081 (Targeted Live Stream Ads)*. This dashboard monitors the raw telemetry from a live HLS stream to ensure **SCTE-35** ad markers are successfully splicing ads into content.
    """)
    
    st.divider()
    
    st.subheader("1. Ad-Buffering (QoE)")
    st.info("""
    **Logic:** `latency > 1000ms` AND `event == 'ad_playing'`
    
    **The Story:** High latency during content is annoying, but during an **Ad**, it costs money. Advertisers demand refunds for buffering ads. This metric protects revenue by flagging "wasted impressions."
    """)

    st.subheader("2. Lost Packets (Billing)")
    st.warning("""
    **Logic:** Gap in `seq_id` (e.g., 50 -> 52)
    
    **The Story:** Ads are billed on "proof of play." If telemetry logs are dropped, we cannot prove the ad was delivered. This check ensures **Revenue Assurance** and data integrity.
    """)

    st.subheader("3. Signal Failure (Eng Health)")
    st.error("""
    **Logic:** `scte35_trigger` fired but `payload` is NULL.
    
    **The Story:** The encoder attempted to signal an ad break but failed to generate the Hex payload. This results in "Dead Air" (Black Screen) because the downstream server doesn't know which ad to insert.
    """)
    
    st.caption("Auto-refreshing every 2 seconds...")

# Initialize History Buffer
df2 = pd.DataFrame()
prev_seq_id = None # Fix: Initialize this variable

# SCTE-35 Mocking (Simplified for single-file demo)
def generate_scte35_payload_mock():
    # Returning a fake hex string for visual simplicity in the demo
    return "0xFC30" + "".join([random.choice("0123456789ABCDEF") for _ in range(8)])

def generate_stream_data():
    # Use global variables to update the history buffer
    global df2, prev_seq_id
    
    sequence_number = 0
    in_ad_break = False
    ad_break_remaining = 0
    
    while True:
        current_time = datetime.now(timezone.utc).isoformat()
        scte35_payload = None
        event_type = "content_playing"
        
        # --- 1. GENERATE DATA ---
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

        # --- 2. PROCESS DATA (Watchdog Logic) ---
        
        # FIFO Logic: Keep buffer at 50 rows
        if len(df2) >= 50:
            df2 = df2.iloc[1:]
            
        # Create new single-row DataFrame
        df = pd.DataFrame([data_packet])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calculate Metrics
        df['ad_buffering'] = (df['latency_ms'] >= 1000) & (df['event'] == 'ad_playing')
        
        # Check for lost packets (if we have a previous ID to compare to)
        if prev_seq_id is not None:
            df['lost_packet'] = df['seq_id'] != (prev_seq_id + 1)
        else:
            df['lost_packet'] = False

        df['signal_failure'] = (df['scte35_payload'] == 'null') & (df['event'] == 'scte35_trigger')
        df['time_only'] = df['timestamp'].dt.strftime('%H:%M:%S')
        
        # Update History
        df2 = pd.concat([df2, df], ignore_index=True)
        
        # Update Previous Sequence for next loop
        prev_seq_id = data_packet['seq_id']

        # --- 3. UPDATE UI ---
        # Draw the chart
        chart_placeholder.line_chart(df2, x='time_only', y='latency_ms')
        
        # --- ALERT LOGIC ---
        # 1. Ad Buffering Check
        buff_count = df2['ad_buffering'].sum()
        if buff_count > 0:
            alert_placeholder.error(f"🔥 CRITICAL: {buff_count} Ad Buffering Events detected!")
        else:
            alert_placeholder.success("✅ Latency Status: Healthy")

        # 2. Lost Packet Check
        lost_count = df2['lost_packet'].sum()
        if lost_count > 0:
            # Create a new placeholder for this specific error so they stack nicely
            st.warning(f"⚠️ DATA LOSS: {lost_count} Packet Gaps detected in sequence.")
        
        # 3. Signal Failure Check
        sig_fail_count = df2['signal_failure'].sum()
        if sig_fail_count > 0:
            st.error(f"🚫 SIGNAL FAILURE: {sig_fail_count} SCTE-35 Triggers failed (Empty Payload).")

        # Increment and Sleep
        sequence_number += 1
        time.sleep(SEGMENT_DURATION)

if __name__ == "__main__":
    generate_stream_data()
import streamlit as st

st.set_page_config(
    page_title="Factor-Based Stock Tool",
    layout="wide"
)

st.sidebar.title("📊 DS5110 – Factor Tool")
st.sidebar.write("Select a page from the left")

st.title("Factor-Based Stock Recommendation Tool")
st.write("Use the sidebar to explore factors or run a backtest.")
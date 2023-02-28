from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window

from io import StringIO
import streamlit as st
import pandas as pd
import json
import numpy as np

st.set_page_config(page_title='Explore Your Data Set',  initial_sidebar_state="auto", menu_items=None)
s = st.session_state
if not s:
        s.pressed_first_button = False

with st.sidebar:
    
    SF_ACCOUNT = st.text_input('Snowflake account:')
    SF_USR = st.text_input('Snowflake user:')
    SF_PWD = st.text_input('Snowflake password:', type='password')

    # conn for learners to use later    
    //conn = {'ACCOUNT': SF_ACCOUNT,'USER': SF_USR,'PASSWORD': SF_PWD}
   # Conn used during dev
    conn = {**st.secrets["snowflake"]}     


# Create a new Snowpark session (or get existing session details)
def create_session():
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(conn).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session
    
   
# Open a Snowflake Snowpark Session
session = create_session()
   
traffic_df = session.sql("select * from usonian_bridges.raw.tacoma_narrows_traffic order by traffic_direction, traffic_date, traffic_hour;").collect()
traffic_df =  pd.DataFrame(traffic_df)
st.write(traffic_df)

st.line_chart(data=traffic_df, *, x=traffic_date, y=traffic_volume, width=0, height=0, use_container_width=True)


st.stop()

                
  

  

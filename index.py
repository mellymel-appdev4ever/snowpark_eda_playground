from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window

from io import StringIO
import streamlit as st
import pandas as pd

st.set_page_config(page_title='Explore Your Data Set',  initial_sidebar_state="auto", menu_items=None)
s = st.session_state
if not s:
        s.pressed_first_button = False

        
with st.sidebar:
    
    SF_ACCOUNT = st.text_input('Snowflake account:')
    SF_USR = st.text_input('Snowflake user:')
    SF_PWD = st.text_input('Snowflake password:', type='password')

    # conn for learners to use later    
    #conn = {'ACCOUNT': SF_ACCOUNT,'USER': SF_USR,'PASSWORD': SF_PWD}
    
    # Conn used during dev
    conn = {**st.secrets["snowflake"]}     
    if st.button('Connect') or s.pressed_first_button:
                   
            session = Session.builder.configs(conn).create()
            s.pressed_first_button = True    
      
            traffic_df = session.sql("select * from usonian_bridges.raw.traffic_by_month order by traffic_month, traffic_direction;").collect()
            # Convert Snowflake DF to Pandas DF so Streamlit can use it in charts
            pd_traffic_df =  pd.DataFrame(traffic_df)
   
            traffic_2_df = session.sql("select DATE_FROM_PARTS(traffic_year, traffic_month, traffic_dom) as date, traffic_volume, traffic_direction from usonian_bridges.raw.tacoma_narrows_traffic;").collect()
            # Convert Snowflake DF to Pandas DF so Streamlit can use it in charts
            pd_traffic_2_df =  pd.DataFrame(traffic_2_df) 


st.vega_lite_chart(pd_traffic_df, {
'mark': 'bar',
'encoding': {
'x': {'field': 'TRAFFIC_MONTH', 'type': 'ordinal'},
'y': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative'},
'color': {'field': 'TRAFFIC_DIRECTION', 'type': 'nominal'}
        }
})

st.vega_lite_chart(pd_traffic_df, {
'mark': 'line',
'encoding': {
'x': {'field': 'TRAFFIC_MONTH', 'type': 'ordinal'},
'y': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative'},
'color': {'field': 'TRAFFIC_DIRECTION', 'type': 'nominal'}
        }
})

st.write(pd_traffic_df) 
   
st.title('Hourly Traffic Volume Scatterplot') 
st.write('Looking for Outlier Volumes')

st.vega_lite_chart(pd_traffic_2_df, {
'mark': 'point',
'encoding': {
'x': {'field': 'TRAFFIC_MONTH', 'type': 'ordinal', 'scale': {'zero': false}},
'y': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative', 'scale': {'zero': false}},
'color': {'field': 'TRAFFIC_DIRECTION', 'type': 'nominal'},
'shape': {'field': 'TRAFFIC_DIRECTION', 'type': 'nominal'}
        }
})
  
        



        
st.stop()

         
  


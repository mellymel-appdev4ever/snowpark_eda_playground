from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window

from io import StringIO
import streamlit as st
import pandas as pd
import json
import numpy as np
import altair as alt

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
      
            traffic_df = session.sql("select traffic_direction, traffic_date, traffic_volume from usonian_bridges.raw.tacoma_narrows_traffic order by traffic_direction, traffic_date, traffic_hour;").collect()
            pd_traffic_df =  pd.DataFrame(traffic_df)
            #st.write(traffic_df)

with st.container():
   st.write("This is inside the container")

   st.write(traffic_df)
   
   st.write(pd_traffic_df)
   
   st.line_chart(traffic_df)

   #c = alt.Chart(traffic_df).mark_circle().encode(x='traffic_date', y='traffic_', size='c', color='c', tooltip=['a', 'b', 'c'])

   #st.altair_chart(c, use_container_width=True)
   #st.line_chart(traffic_df)
        
   #st.area_chart(traffic_df)
   #st.bar_chart(traffic_df)
        
   

st.write("This is outside the container")


        
st.stop()
#st.line_chart(data=traffic_df, *, x=traffic_date, y=traffic_volume, width=0, height=0, use_container_width=True)


st.stop()

#snowflake.snowpark.table.Table            
  


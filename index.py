from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import Window

from io import StringIO
import streamlit as st
import pandas as pd

st.set_page_config(page_title='Explore Your Tacoma Narrows Traffic Data Set',  initial_sidebar_state="auto", menu_items=None)
s = st.session_state
if not s:
        s.pressed_first_button = False

        
with st.sidebar:
    SF_ACCOUNT = st.text_input('Snowflake [Org ID-Acct ID]:')
    SF_USR = st.text_input('Snowflake user:')
    SF_PWD = st.text_input('Snowflake password:', type='password')

    # conn for learners to use later    
    conn = {'ACCOUNT': SF_ACCOUNT,'USER': SF_USR,'PASSWORD': SF_PWD}
    
    # Conn used during dev
    #conn = {**st.secrets["snowflake"]}     
    if st.button('Connect') or s.pressed_first_button:
                 
            session = Session.builder.configs(conn).create()
            st.write('Connection succeeded')
            s.pressed_first_button = True    
            #st.stop()  

            traffic_df = session.sql("select * from usonian_bridges.conformed.v_traffic_by_month order by traffic_month, traffic_direction;").collect()
            pd_traffic_df =  pd.DataFrame(traffic_df)
            st.write('Query of Traffic By Month View Succeeded.')
            
            traffic_2_df = session.sql("select * from usonian_bridges.conformed.v_traffic_by_hour;").collect()
            pd_traffic_2_df =  pd.DataFrame(traffic_2_df)
            st.write('Query of Traffic By Hour View Succeeded.')    
                
with st.container():
    if s.pressed_first_button:
            st.title("Traffic Volume by Month in 2022")

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

            st.title('Traffic Volume by Day of Week') 
            st.write('Where Sunday is 1...and Saturday is 7')


            st.vega_lite_chart(pd_traffic_2_df, {
            'mark': 'boxplot',
            'encoding': {
            'x': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative', 'scale': {'zero': 'false'}},
            'y': {'field': 'TRAFFIC_DOW', 'type': 'ordinal', 'scale': {'zero': 'false'}},
                    }
            }, theme="streamlit", use_container_width=True) 


            st.title('Traffic Volume by Hour of Day') 
            st.write('Where 0 is midnight to 12:59...and 23 is 11:00 to 11:59 ')

            st.vega_lite_chart(pd_traffic_2_df, {
            'mark': 'point',
            'encoding': {
            'x': {'field': 'TRAFFIC_HOUR', 'type': 'ordinal', 'scale': {'zero': 'false'}},
            'y': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative', 'scale': {'zero': 'false'}},
            'color': {'field': 'TRAFFIC_DOW', 'type': 'nominal'},
            'shape': {'field': 'TRAFFIC_DOW', 'type': 'nominal'}
                }
            })

st.stop()

         
  


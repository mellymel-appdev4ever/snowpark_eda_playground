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
      
            traffic_df = session.sql("select * from usonian_bridges.conformed.v_traffic_by_month order by traffic_month, traffic_direction;").collect()
            # Convert Snowflake DF to Pandas DF so Streamlit can use it in charts
            pd_traffic_df =  pd.DataFrame(traffic_df) 
           
with st.container():
   st.write("This is inside the container")
  
   st.write(pd_traffic_df) 
   
   st.line_chart(pd_traffic_df, x='TRAFFIC_MONTH', y='TRAFFIC_VOLUME')
     
   #st.area_chart(pd_traffic_df, x='TRAFFIC_MONTH', y='TRAFFIC_VOLUME')
   #st.bar_chart(pd_traffic_df, x='TRAFFIC_MONTH', y='TRAFFIC_VOLUME')
        
   st.vega_lite_chart(pd_traffic_df, {
    'mark': {'type': 'circle', 'tooltip': True},
    'encoding': {
        'x': {'field': 'TRAFFIC_MONTH', 'type': 'quantitative'},
        'y': {'field': 'TRAFFIC_VOLUME', 'type': 'quantitative'},
        'size': {'field': 'TRAFFIC_VOLUME', 'type': 'nominal'},
        'color': {'field': 'TRAFFIC_VOLUME', 'type': 'nominal'},
    },
   })

   st.vega_lite_chart(pd_traffic_df, {
    'mark': 'line',
    'encoding': {
        'x': {
              'field': 'TRAFFIC_MONTH',
              'type': 'temporal'
           },
        'y': {
              'field': 'TRAFFIC_VOLUME',
              'type': 'quantitative'
            },
        'color': {'field': 'TRAFFIC_DIRECTION', 'type': 'nominal'}
            }
        })

        
   st.vega_lite_chart(pd_traffic_df, [{   
      "transform": [
        {
          "type": "stack",
          "groupby": ["TRAFFIC_MONTH"],
          "sort": {"field": "TRAFFIC_MONTH"},
          "field": "TRAFFIC_VOLUME"
        }
      ]
    }
  ],

  "scales": [
    {
      "name": "TRAFFIC_MONTH",
      "type": "band",
      "range": "width",
      "domain": {"data": "table", "field": "TRAFFIC_MONTH"}
    },
    {
      "name": "TRAFFIC_VOLUME",
      "type": "linear",
      "range": "height",
      "nice": true, "zero": true,
      "domain": {"data": "table", "field": "TRAFFIC_VOLUME"}
    },
    {
      "name": "color",
      "type": "ordinal",
      "range": "category",
      "domain": {"data": "table", "field": "TRAFFIC_DIRECTION"}
    }
  ],

  "axes": [
    {"orient": "bottom", "scale": "TRAFFIC_MONTH", "zindex": 1},
    {"orient": "left", "scale": "TRAFFIC_VOLUME", "zindex": 1}
  ],

  "marks": [
    {
      "type": "rect",
      "from": {"data": "table"},
      "encode": {
        "enter": {
          "x": {"scale": "TRAFFIC_MONTH", "field": "TRAFFIC_MONTH"},
          "width": {"scale": "TRAFFIC_MONTH", "band": 1, "offset": -1},
          "y": {"scale": "TRAFFIC_VOLUME", "field": "TRAFFIC_VOLUME0"},
          "y2": {"scale": "TRAFFIC_VOLUME", "field": "TRAFFIC_VOLUME1"},
          "fill": {"scale": "color", "field": "TRAFFIC_DIRECTION"}
        },
        "update": {
          "fillOpacity": {"value": 1}
        },
        "hover": {
          "fillOpacity": {"value": 0.5}
        }
      }
    }
  ]
}
        
        
st.write("This is outside the container")


        
st.stop()

         
  


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

    conn = {'ACCOUNT': SF_ACCOUNT,'USER': SF_USR,'PASSWORD': SF_PWD}
            
    if st.button('Connect') or s.pressed_first_button:
                   
            session = Session.builder.configs(conn).create()
            s.pressed_first_button = True

            if session != '':
                datawarehouse_list = session.sql("show warehouses;").collect()
                datawarehouse_list =  pd.DataFrame(datawarehouse_list)
                datawarehouse_list= datawarehouse_list["name"]

                datawarehouse_option = st.selectbox('Select Virtual datawarehouse', datawarehouse_list)

                database_list_df = session.sql("show databases;").collect()
                database_list_df =  pd.DataFrame(database_list_df)
                database_list_df = database_list_df["name"]
                
                database_option = st.selectbox('Select database', database_list_df)
                set_database = session.sql(f'''USE DATABASE {database_option}   ;''').collect()

                if set_database != '':
                    set_database = session.use_database(database_option)
                    schema_list_df = session.sql("show schemas;").collect()
                    schema_list_df =  pd.DataFrame(schema_list_df)
                    schema_list_df = schema_list_df["name"]

                    schema_option = st.selectbox('Select schema', schema_list_df)
                    set_schema = session.sql(f'''USE schema {schema_option}   ;''').collect()

                    if set_schema != '':
                        table_list_df = session.sql("show tables;").collect()
                        table_list_df =  pd.DataFrame(table_list_df)
                        if not table_list_df.empty:
                            table_list_df = table_list_df["name"]

                            table_option = st.selectbox('Select tables', table_list_df)
                            upload_table = st.text_input('Use table:',table_option)

                            conn2 = {
                                    'ACCOUNT': SF_ACCOUNT,
                                    'user': SF_USR,
                                    'password': SF_PWD,
                                    'schema': schema_option,
                                    'database': database_option,
                                    'warehouse': datawarehouse_option,
                                }

# Create a new Snowpark session (or get existing session details)
def create_session():
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(conn2).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session
    
   
# Open a Snowflake Snowpark Session
session = create_session()
   
country_codes_df = session.sql("select * from usonian_bridges.raw.tacoma_narrows_traffic order by traffic_direction, traffic_date, traffic_hour;").collect()
country_codes_df =  pd.DataFrame(country_codes_df)
st.write(country_codes_df)

st.stop()
account_locator = st.text_input('Type in Your Snowflake Account Locator')

col1, col2 = st.columns(2)

with col1:
  country_name = st.selectbox(
        "In what country is this bridge located?",
        country_codes_df,
        index=59
  ) 
  #st.write('The country chosen is: ',country_name)
  country_code=country_codes_df.loc[country_codes_df['ISO_COUNTRY_NAME'] == country_name, 'ALPHA_CODE_3DIGIT'].iloc[0]
  st.write('The 3-digit ISO code for', country_name,' is ',country_code, '.')
  
with col2:
   bridge_name = st.text_input('Bridge Name', 'Øresund')
   st.write('The bridge name you entered is:', bridge_name)
   
uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')
if uploaded_file is not None:

    with st.spinner("Uploading image and creating a metadata row about it..."):

        #st.write(uploaded_file)
        file_to_put = getattr(uploaded_file, "name")     
        st.write("File to be Uploaded: " + file_to_put + ".")
        st.image(uploaded_file)
      
        s3 = boto3.client('s3', **st.secrets["s3"])
        bucket = 'uni-bridge-image-uploads'  
        s3.upload_fileobj(uploaded_file, bucket, file_to_put, ExtraArgs={'ContentType': "image/png"})
        
                
        # Write image data in Snowflake table
        df = pd.DataFrame({"ACCOUNT_LOCATOR": [account_locator], "BRIDGE_NAME": [bridge_name], "OG_FILE_NAME": [file_to_put], "COUNTRY_CODE": [country_code]})
        session.write_pandas(df, "UPLOADED_IMAGES")
        
        
        st.write('Thanks for uploading this wonderful image!')
        st.stop()
        
        uploaded_file = None
                
        #st.stop()

  

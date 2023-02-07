import os
from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import xlrd
import snowflake.snowpark
from snowflake.snowpark import Session
from io import StringIO

connection_parameters={
  "account":"ti05946.eu-west-1",
  "user":"TEST",
  "password":"Test@12345",
  "role":"DEMO_ADMIN",
  "database":"TEST",
  "schema":"SCH1"
}

session=Session.builder.configs(connection_parameters).create()
print('Connection Success')
df=session.table('NAME')


#input_path=st.text_input('Enter the file path')
input_file=st.file_uploader("Upload the Mapping document")
if input_file is not None:
  print(input_file.name)
  #with open(os.path.join("tempDir",input_file.name),"wb") as f:
    #f.write(uploadedfile.getbuffer())
  workbook=xlrd.open_workbook(filename=None,file_contents=input_file.read())
  sheet=workbook.sheet_by_index(0)
  col_a=sheet.col_values(0,1)
  col_b=sheet.col_values(1,1)
  col_c=sheet.col_values(2,1)
  join_cols=[]
  for a,b,c in zip(col_a,col_b,col_c):
    if c=='ID':
      join_cols.append(a)
  #print(join_cols)
  for i in join_cols:
    s += "- " + i + "\n"
  st.markdown(s)
  #st.dataframe(df)
    



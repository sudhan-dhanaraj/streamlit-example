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
df=session.table('NAME')
st.dataframe(df)

input_file=st.file_uploader("Upload the Mapping document")
if input_file is not None:
  stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
  data = stringio.read()
  #data=input_file.getvalue()
  workbook=xlrd.open_workbook(data)
  sheet=workbook.sheet_by_index(0)
  col_a=sheet.col_values(0,1)
  col_b=sheet.col_values(1,1)
  col_c=sheet.col_values(2.1)
  join_cols=[]
  for a,b,c in zip(col_a,col_b,col_c):
    if c=='ID':
      join_cols.append(a)
  print(join_cols)
    

"""
# Welcome to Streamlit!

Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:

If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).

In the meantime, below is an example of what you can do with just a few lines of code:
"""


"""with st.echo(code_location='below'):
    total_points = st.slider("Number of points in spiral", 1, 5000, 2000)
    num_turns = st.slider("Number of turns in spiral", 1, 100, 9)

    Point = namedtuple('Point', 'x y')
    data = []

    points_per_turn = total_points / num_turns

    for curr_point_num in range(total_points):
        curr_turn, i = divmod(curr_point_num, points_per_turn)
        angle = (curr_turn + 1) * 2 * math.pi * i / points_per_turn
        radius = curr_point_num / total_points
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        data.append(Point(x, y))

    st.altair_chart(alt.Chart(pd.DataFrame(data), height=500, width=500)
        .mark_circle(color='#0068c9', opacity=0.5)
        .encode(x='x:Q', y='y:Q'))"""

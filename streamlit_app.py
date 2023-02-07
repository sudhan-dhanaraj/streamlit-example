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
db_name='AA_MART_QA'
connection_parameters={
  "account":"ti05946.eu-west-1",
  "user":"Sudhan",
  "password":"Sudhan@9596",
  "role":"ACCOUNTADMIN",
  "database":db_name,
}

session=Session.builder.configs(connection_parameters).create()
print('Connection Success')
df=session.table('NAME')



def df_compare(src_tblname,tgt_tblname,load_type):
         
#Read from excel file and build column_mapping dictionary
  src_tblname=st.text_input("Enter the fully qualified Source Name")
  tgt_tblname=st.text_input("Enter the fully qualified Target Name")
  input_file=st.file_uploader("Upload the Mapping document")
  if input_file is not None and src_tblname is not None and tgt_tblname is not None:
    print(input_file.name)
  #session.file.put(file_path, "@PROC_STAGE/",auto_compress=False)
  #session.file.get(file_path,"tmp/")
  workbook = xlrd.open_workbook(filename=None,file_contents=input_file.read())
  sheet = workbook.sheet_by_index(0)
  col_a = sheet.col_values(0, 1)
  col_b = sheet.col_values(1, 1)
  col_c = sheet.col_values(2, 1)
  join_columns=[]
  src_join_columns=[]
  derived_cols=[]
  column_mapping={}
  #my_dict = {a : b for a, b in zip(col_a, col_b)}
  for a, b, c in zip(col_a, col_b, col_c):
      column_mapping[a]=b
      if c=='ID':
          join_columns.append(a)
          src_join_columns.append(b)
      elif c=='Derived_Column':
          derived_cols.append(a)

  tgt_qualified_name=db_name+'.AA_ANALYTICS.'+tgt_tblname
  src_qualified_name='DL_EXTERNAL_RZ.TRIDENT_ODS.'+src_tblname
  src_col=''
  tgt_col=''
  case_select=''
  for key,val in column_mapping.items():
      if key in derived_cols:
          continue
      src_col=src_col+val+','
      tgt_col=tgt_col+key+','
      case_select=case_select+"CASE WHEN (SRC."+val+" IS NULL AND TGT."+key+" IS NULL) OR (SRC."+val+"=TGT."+key+") THEN 0 ELSE 1 END AS MATCH_"+key+","
  #case_select=case_select+"CASE WHEN SRC."+val+" IS NULL AND TGT."+key+" IS NULL THEN 0 WHEN SRC."+val+"=TGT."+key+" THEN 0 ELSE 1 END AS MATCH_"+key+","

  #case_select=case_select+"CASE WHEN IFNULL(TO_VARCHAR(SRC."+val+"),'')=IFNULL(TO_VARCHAR(TGT."+key+"),'') THEN 0 ELSE 1 END AS MATCH_"+key+","
  src_col=src_col[:-1]
  tgt_col=tgt_col[:-1]
  case_select=case_select[:-1]
  df1_name='Exteranl_RZ'
  df2_name='AA_MART'
  if(load_type=='delta'):
      session.sql("SET DTMPREVIOUS_EXTRACT_DATE=(SELECT PREVIOUS_EXTRACT_DATE FROM (select * from AA_STAGING.STG_DATE_CONTROL where ETL_STATUS_CODE=0 and ETL_STATUS_DESCRIPTION='SUCCEEDED' ORDER BY 7 DESC) LIMIT 1)").collect()
      session.sql("SET DTMCURRENT_EXTRACT_DATE=(SELECT CURRENT_EXTRACT_DATE FROM (select * from AA_STAGING.STG_DATE_CONTROL where ETL_STATUS_CODE=0 and ETL_STATUS_DESCRIPTION='SUCCEEDED' ORDER BY 7 DESC) LIMIT 1)").collect()
      base_df=session.sql("SELECT "+src_col+" from "+src_qualified_name+"  where CAST(UPDATE_DATE AS DATE)>$DTMPREVIOUS_EXTRACT_DATE AND CAST(UPDATE_DATE AS DATE)<=$DTMCURRENT_EXTRACT_DATE")
      compare_df=session.sql("SELECT "+tgt_col+" from "+tgt_qualified_name+"  where CAST(UPDATE_DATE AS DATE)>$DTMPREVIOUS_EXTRACT_DATE AND CAST(UPDATE_DATE AS DATE)<=$DTMCURRENT_EXTRACT_DATE")
  else:
      base_df=session.sql("SELECT "+src_col+" from "+src_qualified_name)
      compare_df=session.sql("SELECT "+tgt_col+" from "+tgt_qualified_name)
  df1=session.table(src_qualified_name)
  df2=session.table(tgt_qualified_name)
  base_row_count=base_df.count()
  compare_row_count=compare_df.count()
  base_col_count=len(base_df.columns)
  compare_col_count=len(compare_df.columns)
  #df_summary=session.create_dataframe([[df1_name, base_col_count,base_row_count],[df2_name,compare_col_count,compare_row_count]],
  #                                schema=["Dataframe","cols","rows"])
  #df_summary.show()

  common_row_count=base_df.intersect(compare_df).count()
  d1=base_df.subtract(compare_df)
  d2=compare_df.subtract(base_df)
  #if d2.count()>0:
  #    print("\nSample Rows available only in new")
  #    d2.select(join_columns).show()
  #if d1.count()>0:
  #    print("\nSample Rows available only in original")
  #    d1.select(src_join_columns).show()
  uneq_cols=[]
  eq_cols=[]
  #Duplicate Check
  print("\n********Duplicate Check********")
  key_cols=",".join(cols for cols in join_columns)
  dupes=session.sql("SELECT "+key_cols+",COUNT(*) AS COUNT FROM "+tgt_qualified_name+" GROUP BY "+key_cols+" HAVING COUNT(*)>1")
  if(dupes.count()>0):
      print("Duplicate Records Found")
      dupes.show()
  else:
      print("No Duplicate Records found")
  #Check Type 2 data
  #query='SELECT '+case_select+' FROM '+src_qualified_name+' SRC JOIN '+tgt_qualified_name+' TGT ON'
  join_condition=" AND ".join("SRC."+s_col+"=TGT."+t_col for s_col,t_col in zip(src_join_columns,join_columns))
  #query='SELECT '+case_select+' FROM '+src_qualified_name+' SRC JOIN '+tgt_qualified_name+' TGT ON '+join_condition
  #print(query)
  if(load_type=='delta'):
      diff=session.sql('SELECT '+case_select+' FROM '+src_qualified_name+' SRC JOIN '+tgt_qualified_name+' TGT ON '+join_condition+"  AND CAST(SRC.UPDATE_DATE AS DATE)>$DTMPREVIOUS_EXTRACT_DATE AND CAST(SRC.UPDATE_DATE AS DATE)<=$DTMCURRENT_EXTRACT_DATE")
  else:
      diff=session.sql('SELECT '+case_select+' FROM '+src_qualified_name+' SRC JOIN '+tgt_qualified_name+' TGT ON '+join_condition)
  #test after the access
  diff.write.save_as_table("TEMP_TBL",mode="overwrite",table_type="temporary")
  diff=session.table('TEMP_TBL')
  diff_cols=''
  kp_dict={}

  for cols in diff.columns:
      #print(cols)
      dat=diff.select(sum(col(cols)).alias(cols)).collect()
      #print(dat)
      val=int(str(dat[0])[str(dat[0]).find('=')+1:-1])
      diff_cols=str(dat[0])[10:str(dat[0]).find('=')]
      kp_dict[diff_cols]=val
  for key,val in kp_dict.items():
      if val==0:
          #eq_cols=eq_cols+key+','
          eq_cols.append(key)
      if val>0:
          #uneq_cols=uneq_cols+key+','
          uneq_cols.append(key)
  #eq_cols=eq_cols[:-1]
  #uneq_cols=uneq_cols[:-1]
  #print(eq_cols)
  #print("\n*********************Column Summary*************************")
  #print("Columns with some values compared different - {}".format(uneq_cols))
  #print("Columns withh all compared values equal - {}".format(eq_cols))

  #show sample mismatch records
  mismatch_list=[]
  for cols in uneq_cols[:3]:
      print("\nSample Mismatch Records for {}".format(cols))
      cl=" , ".join("TGT."+i for i in join_columns)
      if(load_type=='delta'):
          query="SELECT '"+cols+"' AS COLUMN_NAME,"+cl+",CAST(TGT."+cols+" AS VARCHAR) AS TGT_VAL,CAST(SRC."+column_mapping[cols]+" AS VARCHAR) AS SRC_VAL FROM "+tgt_qualified_name+" TGT JOIN "+src_qualified_name+" SRC on "+join_condition+" where TGT."+cols+"<>SRC."+column_mapping[cols]+" AND CAST(SRC.UPDATE_DATE AS DATE)>$DTMPREVIOUS_EXTRACT_DATE AND CAST(SRC.UPDATE_DATE AS DATE)<=$DTMCURRENT_EXTRACT_DATE LIMIT 1"
      else:
          query="SELECT '"+cols+"' AS COLUMN_NAME,"+cl+",CAST(TGT."+cols+" AS VARCHAR) AS TGT_VAL,CAST(SRC."+column_mapping[cols]+" AS VARCHAR) AS SRC_VAL FROM "+tgt_qualified_name+" TGT JOIN "+src_qualified_name+" SRC on "+join_condition+" where TGT."+cols+"<>SRC."+column_mapping[cols]+" LIMIT 1"
     # print("Sample data for {} difference".format(cols))
      #print(query)
      mismatch_list.append(session.sql(query).collect()[0].asDict())
  #f.close()
  #session.file.put("tmp/report.txt","@PROC1_STAGE/",auto_compress=False,overwrite=True)
  session.sql("Select object_agg('Report',object_construct('Table Summary',object_construct('Source Table Name','"+src_tblname+"','Target Table Name','"+tgt_tblname+"','No of columns in source',"+str(len(df1.columns))+",'No of Columns in Target',"+str(len(df2.columns))+"),'Dataframe Summary',object_construct('No of cols considered for Comparison',"+str(base_col_count)+",'No of Rows in Source',"+str(base_row_count)+",'No of Rows in Target',"+str(compare_row_count)+"),'Row Summary',object_construct('No of Rows in common',"+str(common_row_count)+",'No of Rows in DL_External but not in AA_MART',"+str(d1.count())+",'No of Rows in AA_MART but not in DL_EXTERNAL',"+str(d2.count())+"),'Column Summary',object_construct('Columns with some values compared different',"+str(uneq_cols)+",'Columns withh all compared values equal',"+str(eq_cols)+"),'Mismatched Records Sample',"+str(mismatch_list)+"))").collect()[0].asDict()
  #session.sql("INSERT INTO COMPARISON_REPORT(table_name,report,generated_on) Select '"+tgt_tblname+"',object_agg('Report',object_construct('Table Summary',object_construct('Source Table Name','"+src_tblname+"','Target Table Name','"+tgt_tblname+"','No of columns in source',"+str(len(df1.columns))+",'No of Columns in Target',"+str(len(df2.columns))+"),'Dataframe Summary',object_construct('No of cols considered for Comparison',"+str(base_col_count)+",'No of Rows in Source',"+str(base_row_count)+",'No of Rows in Target',"+str(compare_row_count)+"),'Row Summary',object_construct('No of Rows in common',"+str(common_row_count)+",'No of Rows in DL_External but not in AA_MART',"+str(d1.count())+",'No of Rows in AA_MART but not in DL_EXTERNAL',"+str(d2.count())+"),'Column Summary',object_construct('Columns with some values compared different',"+str(uneq_cols)+",'Columns withh all compared values equal',"+str(eq_cols)+"),'Mismatched Records Sample',"+str(mismatch_list)+")),current_timestamp()").collect()
  return "Success"

a=st.button('Historical Comparison')
b=st.button('Incremental Comparison')
if(a):
  #src=st.text_input("Source TableName")
  #tgt=st.text_input("Target TableName")
  #if src is not None and tgt is not None:
  df_compare('full')
  #else:
  #  """Enter a Valid table name"""
elif(b):
  #src=st.button("Source TableName")
  #tgt=st.button("Target TableName")
  #if src is not None and tgt is not None:
  df_compare('delta')
  #else:
  #  """Enter a Valid table name"""
    

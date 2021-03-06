#!/usr/bin/env python3

#pip install xlsxwriter
#pip install Pandas

def sortFunc(e):
  return e['count']

# get param value
def get_param(filepath,param_name,param_pos,ignore="",default_val="Default"):
    if(path.exists(filepath)):
        fileData = open(filepath, "r")
        for line in fileData:
            if(param_name in line):
                if(ignore):
                    if((ignore in line and line.find(ignore)>0) or ignore not in line):
                        default_val = str(line.split()[param_pos].strip())
                else:
                    if(str(line.split()[param_pos].strip())):
                        def_val = str(line.split()[param_pos].strip())
                    return def_val
    return default_val

import os.path
from os import path
import xlsxwriter
import sys

def schemaTag(writeFile,tagType,level,ks,tbl,cql):
  writeFile.writelines(['  - tags:\n','      phase: '+tagType+'_'+level+'_'+ks])
  if level == 'table':
    writeFile.write('_'+tbl)
  writeFile.writelines(['\n','    statements:\n','      - |\n        '+cql+'\n\n'])

def rwTag(writeFile,rwCQL,ks,tbl,tbl_info,ratio='n'):
  if ratio == 'n':
    writeFile.writelines(['  - tags:\n','      phase: '+rwCQL+'_'+ks+'_'+tbl+'\n'])
  elif ratio == 'y':
    writeFile.writelines(['  - tags:\n','      phase: load_'+rwCQL+'_'+ks+'_'+tbl+'\n'])
    ratio_val = str(int(tbl_info['ratio'][rwCQL]*1000))
    writeFile.writelines(['    params:\n','      ratio: ',ratio_val,'\n'])
  writeFile.writelines(['    statements:\n','      - |\n        '])
  field_array = []
  join_info = '},{'+ks+'_'+tbl+'_'
  if rwCQL == 'read':
    cql = 'SELECT * FROM '+ks+'.'+tbl+' WHERE '
    for fld_name,fld_type in tbl_info['field'].items():
      if (fld_name in tbl_info['pk']):
        field_array.append(fld_name+'={'+ks+'_'+tbl+'_'+fld_name+'}')
    field_info = ' AND '.join(map(str, field_array))
    writeFile.write(cql+field_info+'\n\n')
  elif rwCQL == 'write':
    field_array = tbl_info['field'].keys()
    field_names =  ','.join(map(str, field_array))
    field_values =  join_info.join(map(str, field_array))
    cql = 'INSERT INTO '+ks+'.'+tbl+' ('+field_names+') VALUES ({'+ks+'_'+tbl+'_'+field_values+'})'
    writeFile.write(cql+'\n\n')

data_url = []
omit_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs']
headers=["Keyspace","Table","Reads","TPS","% Reads","% RW","","Keyspace","Table","Writes","TPS","% Writes","% RW","","TOTALS"]
headers_width=[11,25,13,9,9,9,3,11,25,13,9,9,9,3,25,13]
read_threshold = 1
write_threshold = 1
include_yaml = 0
new_dc = ''
show_help = ''

for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    show_help = 'y'
  elif(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-rt'):
    read_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-wt'):
    write_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-inc_yaml'):
    include_yaml = 1
  elif(arg=='-new_dc'):
    new_dc = sys.argv[argnum+1]

if show_help:
  help_content = \
  'usage: extract_load.py [-h] [--help] [-inc_yaml]\n'\
  '                       [-p PATH_TO_DIAG_FOLDER]\n'\
  '                       [-rt READ_THRESHOLD]\n'\
  '                       [-wt WRITE_THRESHOLD]\n'\
  'oprional arguments:\n'\
  '-h, --help             This help info\n'\
  '-p                     Path to the diagnostics folder\n'\
  '                        Multiple diag folders accepted\n'\
  '                        i.e. -p PATH1 -p PATH2 -p PATH3\n'\
  '-rt                    Defines percentage of read load\n'\
  '                        to be included in the output\n'\
  '                        Default: 100%\n'\
  '                        i.e. -rt 85\n'\
  '-wt                    Defines percentage of write load\n'\
  '                        to be included in the output\n'\
  '                        Default: 100%\n'\
  '                        i.e. -wt 85\n'
  exit(help_content)

for cluster_url in data_url:
  is_index = 0
  read_subtotal = 0
  write_subtotal = 0
  total_reads = 0
  total_writes = 0
  count = 0
  read_table = {}
  write_table = {}
  write_table2 = {}
  read_count = []
  write_count = []
  table_totals = {}
  total_uptime = 0

  rootPath = cluster_url + "/nodes/"
  for node in os.listdir(rootPath):
    ckpath = rootPath + node + "/nodetool"
    if path.isdir(ckpath):
      iodata = {}
      iodata[node] = {}
      keyspace = ""
      table = ""
      dc = ""
      cfstat = rootPath + node + "/nodetool/cfstats"
      tablestat = rootPath + node + "/nodetool/tablestats"
      clusterpath = rootPath + node + "/nodetool/describecluster"
      infopath = rootPath + node + "/nodetool/info"

      try:
        cfstatFile = open(cfstat, "r")
      except:
        cfstatFile = open(tablestat, "r")
 
      cluster_name = get_param(clusterpath,"Name:",1)
      total_uptime = total_uptime + int(get_param(infopath,"Uptime",3))

      ks = ''

      for line in cfstatFile:
        line = line.strip('\n').strip()
        if("Keyspace" in line):
          ks = line.split(":")[1].strip()
        if ks not in omit_keyspace and ks != '':
          if("Table: " in line):
            tbl = line.split(":")[1].strip()
            is_index = 0
          if("Table (index): " in line):
#            print(ks+":"+tbl)
            is_index = 1
          if("Local read count: " in line):
            count = int(line.split(":")[1].strip())
            if (count > 0):
              total_reads += count
              try:
                type(read_table[ks])
              except:
                read_table[ks] = {}
              try:
                type(read_table[ks][tbl])
                read_table[ks][tbl] += count
              except:
                read_table[ks][tbl] = count
          if (is_index == 0):
            if("Local write count: " in line):
              count = int(line.split(":")[1].strip())
              if (count > 0):
                total_writes += count
                try:
                  type(write_table[ks])
                except:
                  write_table[ks] = {}
                try:
                  type(write_table[ks][tbl])
                  write_table[ks][tbl] += count
                except:
                  write_table[ks][tbl] = count

  schema = rootPath + node + "/driver/schema"
  schemaFile = open(schema, "r")
  ks = ""
  tbl = ""
  create_stmt = {}
  tbl_data = {}
  for line in schemaFile:
    line = line.strip('\n').strip()
    if("CREATE KEYSPACE" in line):
      prev_ks = ks
      ks = line.split()[2].strip('"')
      tbl_data[ks] = {'cql':line}
    elif("CREATE INDEX" in line):
      prev_tbl = tbl
      tbl = line.split()[2].strip('"')
      tbl_data[ks][tbl] = {'type':'Index', 'cql':line}
    elif("CREATE CUSTOM INDEX" in line):
      prev_tbl = tbl
      tbl = line.split()[2].strip('"')
      tbl_data[ks][tbl] = {'type':'Custom Index', 'cql':line}
    elif("CREATE TYPE" in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip()
      tbl = tbl_line.split(".")[1].strip().strip('"')
      tbl_data[ks][tbl] = {'type':'Type', 'cql':line}
      tbl_data[ks][tbl]['field'] = {}
    elif("CREATE TABLE" in line):
      prev_tbl = tbl
      tbl_line = line.split()[2].strip()
      tbl = tbl_line.split(".")[1].strip().strip('"')
      tbl_data[ks][tbl] = {'type':'Table', 'cql':line}
      tbl_data[ks][tbl]['field'] = {}
    elif("CREATE MATERIALIZED VIEW" in line ):
      prev_tbl = tbl
      tbl_line = line.split()[3].strip()
      tbl = tbl_line.split(".")[1].strip().strip('"')
      tbl_data[ks][tbl] = {'type':'Materialized View', 'cql':line}
      tbl_data[ks][tbl]['field'] = {}
    elif("PRIMARY KEY" in line):
      if(line.count('(') == 1):
        tbl_data[ks][tbl]['pk'] = [line.split('(')[1].split(')')[0].split(', ')[0]]
        tbl_data[ks][tbl]['cc'] = line.split('(')[1].split(')')[0].split(', ')
        del tbl_data[ks][tbl]['cc'][0]
      elif(line.count('(') == 2):
        tbl_data[ks][tbl]['pk'] = line.split('(')[2].split(')')[0].split(', ')
        tbl_data[ks][tbl]['cc'] = line.split('(')[2].split(')')[1].lstrip(', ').split(', ')
      tbl_data[ks][tbl]['cql'] += ' ' + line.strip()
    elif line != '' and line.strip() != ');':
      try:
        tbl_data[ks][tbl]['cql'] += ' ' + line
        if('AND ' not in line and ' WITH ' not in line):
          fld_name = line.split()[0]
          fld_type = line.split()[1].strip(',')
          tbl_data[ks][tbl]['field'][fld_name]=fld_type
      except:
        print("Error1:" + ks + "." + tbl + " - " + line)

  for ks,readtable in read_table.items():
    for tablename,tablecount in readtable.items():
      read_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks,writetable in write_table.items():
    for tablename,tablecount in writetable.items():
      write_count.append({'keyspace':ks,'table':tablename,'count':tablecount})

  read_count.sort(reverse=True,key=sortFunc)
  write_count.sort(reverse=True,key=sortFunc)
  total_rw = total_reads+total_writes

  #Create Cluster GC Spreadsheet
  workbook = xlsxwriter.Workbook(cluster_url + "/" + cluster_name + "_" + "workload" + '.xlsx')
  worksheet = workbook.add_worksheet('RW Data')

  column=0
  for col_width in headers_width:
    worksheet.set_column(column,column,col_width)
    column+=1

  header_format1 = workbook.add_format({
      'bold': True,
      'italic' : True,
      'text_wrap': False,
      'font_size': 14,
      'border': 1,
      'valign': 'top'})

  header_format2 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 12,
      'border': 1,
      'valign': 'top'})

  header_format3 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})
      
  data_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'valign': 'top'})

  data_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'italic': True,
      'valign': 'top'})

  row=0
  column=0
  for header in headers:
      if header == '':
        worksheet.write(row,column,header)
      else:
        worksheet.write(row,column,header,header_format1)
      column+=1
  
  perc_reads = 0.0
  row = 1
  column = 0
  for reads in read_count:
    perc_reads = float(read_subtotal) / float(total_reads)
    if (perc_reads <= read_threshold):
      ks = reads['keyspace']
      tbl = reads['table']
      cnt = reads['count']
      try:
        type(table_totals[ks])
      except:
        table_totals[ks] = {}
      table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
      read_subtotal += cnt
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,data_format)
      worksheet.write(row,column+3,round(float(cnt)/total_uptime,3),data_format)
      worksheet.write(row,column+4,round(float(cnt)/total_reads*100,3),data_format)
      worksheet.write(row,column+5,round(float(cnt)/float(total_rw)*100,3),data_format)
      row+=1

  perc_writes = 0.0
  row = 1
  column = 7
  for writes in write_count:
    perc_writes = float(write_subtotal) / float(total_writes)
    if (perc_writes <= write_threshold):
      ks = writes['keyspace']
      tbl = writes['table']
      cnt = writes['count']
      try:
        type(table_totals[ks])
      except:
        table_totals[ks] = {}
      try:
        type(table_totals[ks][tbl])
        table_totals[ks][tbl] = {'reads':table_totals[ks][tbl]['reads'],'writes':cnt}
      except:
        table_totals[ks][tbl] = {'reads':'n/a','writes':cnt}
      write_subtotal += writes['count']
      worksheet.write(row,column,ks,data_format)
      worksheet.write(row,column+1,tbl,data_format)
      worksheet.write(row,column+2,cnt,data_format)
      worksheet.write(row,column+3,round(float(cnt)/total_uptime,3),data_format)
      worksheet.write(row,column+4,round(float(cnt)/total_writes*100,3),data_format)
      worksheet.write(row,column+5,round(float(cnt)/float(total_rw)*100,3),data_format)
      row+=1

  total_tps = float(total_rw)/total_uptime
  total_tpd = total_tps*60*60*24
  total_tpmo = total_tps*60*60*24*365/12
  days_uptime = total_uptime/60/60/24

  if (total_tpd>=1000000000):
    tpd_value = total_tpd/1000000000
    tpd_unit = 'Billions'
  elif (total_tpd>=1000000):
    tpd_value = total_tpd/1000000
    tpd_unit = 'Millions'
  elif (total_tpd>=1000):
    tpd_value = total_tpd/1000
    tpd_unit = 'Thousands'

  if (total_tpmo>=1000000000000):
    tpmo_value = total_tpmo/1000000000000
    tpmo_unit = 'Trillions'
  elif (total_tpmo>=1000000000):
    tpmo_value = total_tpmo/1000000000
    tpmo_unit = 'Billions'
  elif (total_tpmo>=1000000):
    tpmo_value = total_tpmo/1000000
    tpmo_unit = 'Millions'
  elif (total_tpmo>=1000):
    tpmo_value = total_tpmo/1000
    tpmo_unit = 'Thousands'


  column=14
  worksheet.write(1,column,'Reads',header_format3)
  worksheet.write(1,column+1,total_reads,data_format)
  worksheet.write(2,column,'Reads TPS',header_format3)
  worksheet.write(2,column+1,round(total_reads/total_uptime,3),data_format)
  worksheet.write(3,column,'Reads % RW',header_format3)
  worksheet.write(3,column+1,round(total_reads/float(total_rw)*100,3),data_format)
  worksheet.write(4,column,'Writes',header_format3)
  worksheet.write(4,column+1,total_writes,data_format)
  worksheet.write(5,column,'Writes TPS',header_format3)
  worksheet.write(5,column+1,round(total_writes/total_uptime,3),data_format)
  worksheet.write(6,column,'Writes % RW',header_format3)
  worksheet.write(6,column+1,round(total_writes/float(total_rw)*100,3),data_format)
  worksheet.write(7,column,'RW',header_format3)
  worksheet.write(7,column+1,total_rw,data_format)
  worksheet.write(8,column,'Total Log Time (Seconds)',header_format3)
  worksheet.write(8,column+1,total_uptime,data_format)
  worksheet.write(9,column,'Total Log Time (Days)',header_format3)
  worksheet.write(9,column+1,days_uptime,data_format)
  worksheet.write(10,column,'Application TPS',header_format3)
  worksheet.write(10,column+1,round(total_tps,3),data_format)
  worksheet.write(11,column,'Application TPD ('+tpd_unit+')',header_format3)
  worksheet.write(11,column+1,round(tpd_value,3),data_format)
  worksheet.write(12,column,'Application *TPMO ('+tpmo_unit+')',header_format3)
  worksheet.write(12,column+1,round(tpmo_value,3),data_format)
  worksheet.write(14,column,'NOTE: Transaction totals include all nodes (nodetool cfstats)',data_format2)
  worksheet.write(15,column,'* Uptimes is the total all node uptimes (nodetool info)',data_format2)
  worksheet.write(16,column,'** TPMO (transactions per month) is calculated at 30.41667 days (365/12)',data_format2)
  workbook.close()
exit();

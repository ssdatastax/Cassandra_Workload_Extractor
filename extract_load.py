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
system_keyspace = ['OpsCenter','dse_insights_local','solr_admin','test','dse_system','dse_analytics','system_auth','system_traces','system','dse_system_local','system_distributed','system_schema','dse_perf','dse_insights','dse_security','killrvideo','dse_leases','dsefs_c4z','HiveMetaStore','dse_analytics','dsefs']
headers=["Keyspace","Table","Table Size","","Keyspace","Table","Total Read Req","Average TPS","% Reads","% RW","","Keyspace","Table","Total Write Req","Average TPS","% Writes","% RW","","TOTALS"]
headers_width=[14,25,17,3,14,25,17,13,9,9,3,14,25,17,13,9,9,3,25,20]
ks_type_abbr = {'app':'Application','sys':'System'}
read_threshold = 1
write_threshold = 1
include_yaml = 0
new_dc = ''
show_help = ''
include_system = 0
    
for argnum,arg in enumerate(sys.argv):
  if(arg=='-h' or arg =='--help'):
    show_help = 'y'
  elif(arg=='-p'):
    data_url.append(sys.argv[argnum+1])
  elif(arg=='-rt'):
    read_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-wt'):
    write_threshold = float(sys.argv[argnum+1])/100
  elif(arg=='-sys'):
    include_system = 1

if (include_system): ks_type_array=['app','sys']
else: ks_type_array=['app']

if show_help:
  help_content = \
  'usage: extract_load.py [-h] [--help] [-inc_yaml]\n'\
  '                       [-p PATH_TO_DIAG_FOLDER]\n'\
  '                       [-rt READ_THRESHOLD]\n'\
  '                       [-wt WRITE_THRESHOLD]\n'\
  '                       [-sys INCLUDE SYSTEM KEYSPACES]\n'\
  'optional arguments:\n'\
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
  '-sys                   Include System files in addtional tab\n'\

  exit(help_content)

for cluster_url in data_url:
  is_index = 0
  read_subtotal = {'app':0,'sys':0}
  write_subtotal = {'app':0,'sys':0}
  total_size = {'app':0,'sys':0}
  total_reads = {'app':0,'sys':0}
  total_writes = {'app':0,'sys':0}
  read_count = {'app':[],'sys':[]}
  write_count = {'app':[],'sys':[]}
  total_rw = {'app':0,'sys':0}
  count = 0
  size_table = {'app':{},'sys':{}}
  read_table = {}
  write_table = {}
  size_totals = {}
  table_totals = {}
  total_uptime = 0
  table_tps={}
  node_uptime = {}

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
      node_uptime[node] = int(get_param(infopath,'Uptime',3))
      total_uptime = total_uptime + int(node_uptime[node])

      ks = ''
      tbl = ''
      for line in cfstatFile:
        line = line.strip('\n').strip()
        if (line==""): tbl = "";
        if("Keyspace" in line):
          ks = line.split(":")[1].strip()
        if ks not in system_keyspace and ks != '': ks_type='app'
        else: ks_type='sys'
        try:
          type(table_tps[ks])
        except:
          table_tps[ks]={}
        if("Table: " in line):
          tbl = line.split(":")[1].strip()
          is_index = 0
        elif("Table (index): " in line):
          tbl = line.split(":")[1].strip()
          is_index = 1
        try:
          type(table_tps[ks][tbl])
        except:
          table_tps[ks][tbl]={}
        if (tbl and "Space used (live): " in line):
          tsize = int(line.split(":")[1].strip())
          if (tsize > 0):
            total_size[ks_type] += tsize
            try:
              type(size_table[ks_type][ks])
            except:
              size_table[ks_type][ks] = {}
            try:
              type(size_table[ks_type][ks][tbl])
              size_table[ks_type][ks][tbl] += tsize
            except:
              size_table[ks_type][ks][tbl] = tsize
        if(tbl and "Local read count: " in line):
          count = int(line.split(":")[1].strip())
          if (count > 0):
            total_reads[ks_type] += count
            try:
              table_tps[ks][tbl]['read'] += float(count) / float(node_uptime[node])
            except:
              table_tps[ks][tbl]['read'] = float(count) / float(node_uptime[node])
            try:
              type(read_table[ks])
            except:
              read_table[ks] = {}
            try:
              type(read_table[ks][tbl])
              read_table[ks][tbl] += count
            except:
              read_table[ks][tbl] = count
        if (tbl and is_index == 0):
          if("Local write count: " in line):
            count = int(line.split(":")[1].strip())
            if (count > 0):
              total_writes[ks_type] += count
              try:
                table_tps[ks][tbl]['write'] += float(count) / float(node_uptime[node])
              except:
                table_tps[ks][tbl]['write'] = float(count) / float(node_uptime[node])
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
    if ks not in system_keyspace and ks != '': ks_type='app'
    else: ks_type='sys'
    for tablename,tablecount in readtable.items():
      read_count[ks_type].append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks,writetable in write_table.items():
    if ks not in system_keyspace and ks != '': ks_type='app'
    else: ks_type='sys'
    for tablename,tablecount in writetable.items():
      write_count[ks_type].append({'keyspace':ks,'table':tablename,'count':tablecount})

  for ks_type in ks_type_array:
    read_count[ks_type].sort(reverse=True,key=sortFunc)
    write_count[ks_type].sort(reverse=True,key=sortFunc)
    total_rw[ks_type] = total_reads[ks_type]+total_writes[ks_type]

  #Create Cluster GC Spreadsheet
  worksheet = {}
  workbook = xlsxwriter.Workbook(cluster_url + "/" + cluster_name + "_" + "workload" + '.xlsx')
  for ks_type in ks_type_array:
    worksheet[ks_type] = workbook.add_worksheet(ks_type_abbr[ks_type] + ' Workload')

    column=0
    for col_width in headers_width:
      worksheet[ks_type].set_column(column,column,col_width)
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
      
  header_format4 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
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
      
  perc_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,###.00%',
      'valign': 'top'})

  num_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,###',
      'valign': 'top'})

  num_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '#,##0.00',
      'valign': 'top'})

  num_format3 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'num_format': '#,###',
      'valign': 'top'})

  title_format = workbook.add_format({
      'bold': 1,
      'font_size': 13,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#EB6C34'})

  title_format2 = workbook.add_format({
      'bold': 1,
      'font_size': 13,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#998E5D'})
      
  title_format3 = workbook.add_format({
      'bold': 1,
      'font_size': 14,
      'border': 1,
      'align': 'center',
      'valign': 'vcenter',
      'font_color': 'white',
      'bg_color': '#3A3A42'})

  for ks_type in ks_type_array:
    worksheet[ks_type].merge_range('A1:T1', 'Workload for '+cluster_name, title_format3)
    worksheet[ks_type].merge_range('A2:C2', 'Table Size', title_format)
    worksheet[ks_type].merge_range('E2:J2', 'Read Requests', title_format)
    worksheet[ks_type].merge_range('L2:Q2', 'Write Requests', title_format)
    worksheet[ks_type].merge_range('S2:T2', 'Totals', title_format)

  for ks_type in ks_type_array:
    column=0
    for header in headers:
        if header == '':
          worksheet[ks_type].write(2,column,header)
        else:
          worksheet[ks_type].write(2,column,header,header_format1)
        column+=1

  last_row = 0
  total_row = {'size':0,'read_tps':0,'write_tps':0}


  for ks_type in ks_type_array:
    row = {'app':3,'sys':3}
    perc_reads = 0.0
    column = 0
    for ks,t_data in size_table[ks_type].items():
      for tbl,t_size in t_data.items():
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,t_size,num_format1)
        row[ks_type]+=1

    total_row['size'] = row[ks_type]
    last_row = row[ks_type]

    row = {'app':3,'sys':3}
    perc_reads = 0.0
    column = 4
    for reads in read_count[ks_type]:
      perc_reads = float(read_subtotal[ks_type]) / float(total_reads[ks_type])
      if (perc_reads <= read_threshold):
        ks = reads['keyspace']
        tbl = reads['table']
        cnt = reads['count']
        try:
          type(table_totals[ks])
        except:
          table_totals[ks] = {}
        table_totals[ks][tbl] = {'reads':cnt,'writes':'n/a'}
        read_subtotal[ks_type] += cnt
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,num_format1)
        worksheet[ks_type].write(row[ks_type],column+3,table_tps[ks][tbl]['read'],num_format2)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_reads[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1
  
    total_row['read'] = row[ks_type]
    if (last_row<row[ks_type]): last_row=row[ks_type]

    perc_writes = 0.0
    row = {'app':3,'sys':3}
    column = 11
    for writes in write_count[ks_type]:
      perc_writes = float(write_subtotal[ks_type]) / float(total_writes[ks_type])
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
        write_subtotal[ks_type] += cnt
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,num_format1)
        worksheet[ks_type].write(row[ks_type],column+3,table_tps[ks][tbl]['write'],num_format2)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_writes[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1

    total_row['write'] = row[ks_type]
    if (last_row<row[ks_type]): last_row=row[ks_type]
    if (last_row<16): last_row=16
    worksheet[ks_type].merge_range('A'+str(last_row+3)+':D'+str(last_row+3), 'NOTES', title_format2)
    worksheet[ks_type].merge_range('A'+str(last_row+4)+':D'+str(last_row+4), 'Transaction totals (Reads/Writes) include all nodes (nodetool cfstats)', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+5)+':D'+str(last_row+5), 'Log Times (which is used to calculate TPS...) is a sum of the uptimes of all nodes', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+6)+':D'+str(last_row+6), '% RW is the Read or Write % of the total reads and writes', data_format)
    worksheet[ks_type].merge_range('A'+str(last_row+7)+':D'+str(last_row+7), '* TPMO - transactions per month is calculated at 30.4375 days (365.25/12)', data_format)

    reads_tps = total_reads[ks_type]/total_uptime
    reads_tpd = reads_tps*60*60*24
    reads_tpmo = reads_tps*60*60*24*365.25/12

    writes_tps = total_writes[ks_type]/total_uptime
    writes_tpd = writes_tps*60*60*24
    writes_tpmo = writes_tps*60*60*24*365.25/12

    total_tps = float(total_rw[ks_type])/total_uptime
    total_tpd = total_tps*60*60*24
    total_tpmo = total_tps*60*60*24*365.25/12
    days_uptime = total_uptime/60/60/24

    row=1
    column=18
    worksheet[ks_type].write(row+1,column,'Read Requests',header_format4)
    worksheet[ks_type].write(row+1,column+1,'=SUM(G4:G'+ str(total_row['read'])+')',num_format3)
    worksheet[ks_type].write(row+2,column,'Avg Read TPS',header_format3)
    worksheet[ks_type].write(row+2,column+1,'=SUM(H4:H'+ str(total_row['read'])+')',num_format1)
    worksheet[ks_type].write(row+3,column,'Avg Read TPD (K)',header_format3)
    worksheet[ks_type].write(row+3,column+1,'=T4*60*60*24/1000',num_format1)
    worksheet[ks_type].write(row+4,column,'Avg Read TPMO* (M)',header_format3)
    worksheet[ks_type].write(row+4,column+1,'=T5*365/12/1000',num_format1)
    worksheet[ks_type].write(row+5,column,'Reads % RW',header_format3)
    worksheet[ks_type].write(row+5,column+1,'=T3/(T3+T8)',perc_format)
    worksheet[ks_type].write(row+6,column,'Write Requests',header_format4)
    worksheet[ks_type].write(row+6,column+1,'=SUM(N4:N'+ str(total_row['write'])+')',num_format3)
    worksheet[ks_type].write(row+7,column,'Avg Write TPS',header_format3)
    worksheet[ks_type].write(row+7,column+1,'=SUM(O4:O'+ str(total_row['write'])+')',num_format1)
    worksheet[ks_type].write(row+8,column,'Avg Write TPD (K)',header_format3)
    worksheet[ks_type].write(row+8,column+1,'=T9*60*60*24/1000',num_format1)
    worksheet[ks_type].write(row+9,column,'Avg Write TPMO* (M)',header_format3)
    worksheet[ks_type].write(row+9,column+1,'=T10*365/12/1000',num_format1)
    worksheet[ks_type].write(row+10,column,'Writes % RW',header_format3)
    worksheet[ks_type].write(row+10,column+1,'=T8/(T3+T8)',perc_format)
    worksheet[ks_type].write(row+11,column,'Total RW (Reads+Writes)',header_format4)
    worksheet[ks_type].write(row+11,column+1,total_rw[ks_type],num_format3)
    worksheet[ks_type].write(row+12,column,'Total Avg TPS',header_format3)
    worksheet[ks_type].write(row+12,column+1,'=T4+T9',num_format1)
    worksheet[ks_type].write(row+13,column,'Total Avg TPD (K)',header_format3)
    worksheet[ks_type].write(row+13,column+1,'=T5+T10',num_format1)
    worksheet[ks_type].write(row+14,column,'Total Avg TPMO* (M)',header_format3)
    worksheet[ks_type].write(row+14,column+1,'=T6+T11',num_format1)
    worksheet[ks_type].write(row+15,column,ks_type_abbr[ks_type] + ' Data Size (GB)',header_format4)
    worksheet[ks_type].write(row+15,column+1,'=SUM(C4:C'+ str(total_row['size'])+')/1000000000',num_format3)

  workbook.close()
exit();


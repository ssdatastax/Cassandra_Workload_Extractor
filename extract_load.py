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
    try:
      type(dc_array[dc])
    except:
      dc_array.append(dc)
  return default_val

# collect the dc name for each node
def get_dc(statuspath):
  if(path.exists(statuspath)):
    statusFile = open(statuspath, 'r')
    dc = ''
    node = ''
    for line in statusFile:
      if('Datacenter:' in line):
        dc = str(line.split(':')[1].strip())
        if dc not in dc_array:
          dc_array.append(dc)
      elif(line.count('.')>=3):
        node = str(line.split()[1].strip())
        node_dc[node] = dc
  else:
    exclude_tab.append('node')

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
headers=["Keyspace","Table","Table Size","","Keyspace","Table","Total Read Req","Average TPS","% Reads","% RW","","Keyspace","Table","Total Write Req","Average TPS","% Writes","% RW","","TOTALS","Copy/Paste"]
headers_width=[14,25,17,3,14,25,17,13,9,9,3,14,25,17,13,9,9,3,25,15,20]
ks_type_abbr = {'app':'Application','sys':'System','clu':'Cluster'}
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
else: ks_type_array=['clu']

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
  '-sys                   Separate Application and System files\n'\
  '                        in seperate spreadsheet tabs\n'\

  exit(help_content)

for cluster_url in data_url:
  is_index = 0
  read_subtotal = {'app':0,'sys':0,'clu':0}
  write_subtotal = {'app':0,'sys':0,'clu':0}
  total_size = {'app':0,'sys':0,'clu':0}
  total_reads = {'app':0,'sys':0,'clu':0}
  total_writes = {'app':0,'sys':0,'clu':0}
  read_count = {'app':[],'sys':[],'clu':[]}
  write_count = {'app':[],'sys':[],'clu':[]}
  table_count = {'app':[],'sys':[],'clu':[]}
  total_rw = {'app':0,'sys':0,'clu':0}
  count = 0
  size_table = {'app':{},'sys':{},'clu':{}}
  read_table = {}
  write_table = {}
  size_totals = {}
  table_totals = {}
  total_uptime = 0
  table_tps={}
  node_uptime = {}
  dc_array = []
  node_dc = {}
  table_array = {}

  rootPath = cluster_url + "/nodes/"
  for node in os.listdir(rootPath):
    ckpath = rootPath + node + "/nodetool"
    if path.isdir(ckpath):

      statuspath = rootPath + node + '/nodetool/status'
      get_dc(statuspath)
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
          tbl_data[ks] = {'cql':line,'rf':0}
          rf=0;
          for dc_name in dc_array:
            if ("'"+dc_name+"':" in line):
              i=0
              for prt in line.split():
                prt_chk = "'"+dc_name+"':"
                if (prt==prt_chk):
                  rf=line.split()[i+1].strip('}').strip(',').strip("'")
                  tbl_data[ks]['rf']+=float(rf)
                i+=1
            elif("'replication_factor':" in line):
              i=0
              for prt in line.split():
                prt_chk = "'replication_factor':"
                if (prt==prt_chk):
                  rf=line.split()[i+1].strip('}').strip(',').strip("'")
                  tbl_data[ks]['rf']+=float(rf)
                i+=1
            else:tbl_data[ks]['rf']=float(1)

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
          table_array[ks]=[]
        if include_system==1:
          if ks not in system_keyspace and ks != '': ks_type='app'
          else: ks_type='sys'
        else: ks_type='clu'
        
        try:
          type(table_tps[ks])
        except:
          table_tps[ks]={}
        if("Table: " in line):
          tbl = line.split(":")[1].strip()
          is_index = 0
          table_array[ks].append(tbl)
        elif("Table (index): " in line):
          tbl = line.split(":")[1].strip()
          is_index = 1
          table_array[ks].append(tbl)
        try:
          type(table_tps[ks][tbl])
        except:
          table_tps[ks][tbl]={}
        if (tbl and "Space used (live):" in line or "Memtable data size:" in line):
          tsize = int(line.split(":")[1].strip())
          if (tsize > 0):
            total_size[ks_type] += tsize
            try:
              type(size_table[ks])
            except:
              size_table[ks] = {}
            try:
              type(size_table[ks][tbl])
              size_table[ks][tbl] += tsize
            except:
              size_table[ks][tbl] = tsize
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
  for ks,tbl_array in table_array.items():
#      print(ks+'.'+str(tbl))
#  for ks,sizetable in size_table.items():
    if include_system==1:
      if ks not in system_keyspace and ks != '': ks_type='app'
      else: ks_type='sys'
    else: ks_type='clu'
    for tbl in tbl_array:
#    for tbl,tblsize in sizetable.items():
      try:
        table_count[ks_type].append({'keyspace':ks,'table':tbl,'count':size_table[ks][tbl]})
      except:
        table_count[ks_type].append({'keyspace':ks,'table':tbl,'count':0})
  for ks,tbl_array in table_array.items():
#  for ks,readtable in read_table.items():
    if include_system==1:
      if ks not in system_keyspace and ks != '': ks_type='app'
      else: ks_type='sys'
    else: ks_type='clu'
    for tbl in tbl_array:
 #   for tbl,tablecount in readtable.items():
      try:
        read_count[ks_type].append({'keyspace':ks,'table':tbl,'count':read_table[ks][tbl]})
      except:
        read_count[ks_type].append({'keyspace':ks,'table':tbl,'count':0})
  for ks,tbl_array in table_array.items():
#  for ks,writetable in write_table.items():
    if include_system==1:
      if ks not in system_keyspace and ks != '': ks_type='app'
      else: ks_type='sys'
    else: ks_type='clu'
    for tbl in tbl_array:
#    for tbl,tablecount in writetable.items():
      try:
        write_count[ks_type].append({'keyspace':ks,'table':tbl,'count':write_table[ks][tbl]})
      except:
        write_count[ks_type].append({'keyspace':ks,'table':tbl,'count':0})

  for ks_type in ks_type_array:
    read_count[ks_type].sort(reverse=True,key=sortFunc)
    write_count[ks_type].sort(reverse=True,key=sortFunc)
    table_count[ks_type].sort(reverse=True,key=sortFunc)
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

  num_format4 = workbook.add_format({
      'bold': True,
      'text_wrap': False,
      'font_size': 11,
      'border': 6,
      'border_color': '#998E5D',
      'num_format': '#,###',
      'valign': 'top'})

  total_format1 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," MB";[>999]0.00," KB";0',
      'valign': 'top'})

  total_format2 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'num_format': '[>999999]#,##0.00,," MB";[>999]0.00," KB";0',
      'valign': 'top'})

  total_format3 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," M";[>999]0.00," K";0',
      'valign': 'top'})

  total_format4 = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'font_color': 'white',
      'bg_color': '#3980D3',
      'num_format': '[>999999]#,##0.00,," M";[>999]0.00," K";0',
      'valign': 'top'})

  tps_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," (M)TPS";[>999]0.00," (K)TPS";0" TPS"',
      'valign': 'top'})

  tpmo_format = workbook.add_format({
      'text_wrap': False,
      'font_size': 11,
      'border': 1,
      'num_format': '[>999999]#,##0.00,," (M)TPMo";[>999]0.00," (K)TPMo";0" TPMo"',
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
    worksheet[ks_type].merge_range('A1:U1', ks_type_abbr[ks_type] + ' Workload for ' + cluster_name, title_format3)
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
  rf=0

  for ks_type in ks_type_array:
    row = {'app':3,'sys':3,'clu':3}
    perc_reads = 0.0
    column = 0
    for sizes in table_count[ks_type]:
      ks = sizes['keyspace']
      tbl = sizes['table']
      t_size = sizes['count']
      try: rf=tbl_data[ks]['rf']
      except: rf = 1
      worksheet[ks_type].write(row[ks_type],column,ks,data_format)
      worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
      worksheet[ks_type].write(row[ks_type],column+2,float(t_size)/rf,total_format1)
      row[ks_type]+=1

    total_row['size'] = row[ks_type]

    row = {'app':3,'sys':3,'clu':3}
    perc_reads = 0.0
    column = 4
    for reads in read_count[ks_type]:
      perc_reads = float(read_subtotal[ks_type]) / float(total_reads[ks_type])
      if (perc_reads <= read_threshold):
        ks = reads['keyspace']
        tbl = reads['table']
        cnt = reads['count']
        read_subtotal[ks_type] += cnt
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,total_format3)
        try:
          worksheet[ks_type].write(row[ks_type],column+3,table_tps[ks][tbl]['read']/2,tps_format)
        except:
          worksheet[ks_type].write(row[ks_type],column+3,0,tps_format)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_reads[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1
  
    total_row['read'] = row[ks_type]

    perc_writes = 0.0
    row = {'app':3,'sys':3,'clu':3}
    column = 11
    for writes in write_count[ks_type]:
      ks = writes['keyspace']
      tbl = writes['table']
      cnt = writes['count']
      perc_writes = float(write_subtotal[ks_type]) / float(total_writes[ks_type])
      try: rf=tbl_data[ks]['rf']
      except: rf=1
      if (perc_writes <= write_threshold):
        worksheet[ks_type].write(row[ks_type],column,ks,data_format)
        worksheet[ks_type].write(row[ks_type],column+1,tbl,data_format)
        worksheet[ks_type].write(row[ks_type],column+2,cnt,total_format3)
        try:
          worksheet[ks_type].write(row[ks_type],column+3,table_tps[ks][tbl]['write']/rf,tps_format)
        except:
          worksheet[ks_type].write(row[ks_type],column+3,0,tps_format)
        worksheet[ks_type].write(row[ks_type],column+4,float(cnt)/total_writes[ks_type],perc_format)
        worksheet[ks_type].write(row[ks_type],column+5,float(cnt)/float(total_rw[ks_type]),perc_format)
        row[ks_type]+=1

    total_row['write'] = row[ks_type]

    row=1
    column=18
    worksheet[ks_type].write(row,column+2,'Copy/Paste',title_format)
    worksheet[ks_type].write(row+1,column,'Read Requests',header_format4)
    worksheet[ks_type].write(row+1,column+1,'=SUM(G4:G'+ str(total_row['read'])+')',total_format3)
    worksheet[ks_type].write(row+1,column+2,'=SUM(G4:G'+ str(total_row['read'])+')',num_format1)
    worksheet[ks_type].write(row+2,column,'Avg Read TPS',header_format4)
    worksheet[ks_type].write(row+2,column+1,'=SUM(H4:H'+ str(total_row['read'])+')',tps_format)
    worksheet[ks_type].write(row+2,column+2,'=SUM(H4:H'+ str(total_row['read'])+')',num_format4)
    worksheet[ks_type].write(row+3,column,'Avg Read TPMO',header_format4)
    worksheet[ks_type].write(row+3,column+1,'=T4*365.25/12',tpmo_format)
    worksheet[ks_type].write(row+3,column+2,'=T4*365.25/12',num_format1)
    worksheet[ks_type].write(row+4,column,'Reads % RW',header_format4)
    worksheet[ks_type].write(row+4,column+1,'=T3/(T3+T7)',perc_format)
    header_format4.set_top(2)
    total_format1.set_top(2)
    num_format1.set_top(2)
    worksheet[ks_type].write(row+5,column,'Write Requests',header_format4)
    worksheet[ks_type].write(row+5,column+1,'=SUM(N4:N'+ str(total_row['write'])+')',total_format3)
    worksheet[ks_type].write(row+5,column+2,'=SUM(N4:N'+ str(total_row['write'])+')',num_format1)
    header_format4.set_top(1)
    total_format1.set_top(1)
    num_format1.set_top(1)
    worksheet[ks_type].write(row+6,column,'Avg Write TPS',header_format4)
    worksheet[ks_type].write(row+6,column+1,'=SUM(O4:O'+ str(total_row['write'])+')',tps_format)
    worksheet[ks_type].write(row+6,column+2,'=SUM(O4:O'+ str(total_row['write'])+')',num_format4)
    worksheet[ks_type].write(row+7,column,'Avg Write TPMO',header_format4)
    worksheet[ks_type].write(row+7,column+1,'=T8*365.25/12',tpmo_format)
    worksheet[ks_type].write(row+7,column+2,'=T8*365.25/12',num_format1)
    worksheet[ks_type].write(row+8,column,'Writes % RW',header_format4)
    worksheet[ks_type].write(row+8,column+1,'=T7/(T3+T7)',perc_format)
    header_format4.set_top(2)
    total_format1.set_top(2)
    num_format1.set_top(2)
    worksheet[ks_type].write(row+9,column,'Total RW',header_format4)
    worksheet[ks_type].write(row+9,column+1,'=T3+T7',total_format3)
    worksheet[ks_type].write(row+9,column+2,'=T3+T7',num_format1)
    header_format4.set_top(1)
    total_format1.set_top(1)
    num_format1.set_top(1)
    worksheet[ks_type].write(row+10,column,'Total Avg TPS',header_format4)
    worksheet[ks_type].write(row+10,column+1,'=T4+T8',tps_format)
    worksheet[ks_type].write(row+10,column+2,'=T4+T8',num_format1)
    worksheet[ks_type].write(row+11,column,'Total Avg TPMO',header_format4)
    worksheet[ks_type].write(row+11,column+1,'=T5+T9',tpmo_format)
    worksheet[ks_type].write(row+11,column+2,'=T5+T9',num_format1)
    worksheet[ks_type].write(row+12,column,ks_type_abbr[ks_type] + ' Data Size',header_format4)
    worksheet[ks_type].write(row+12,column+1,'=SUM(C4:C'+ str(total_row['size'])+')',total_format1)
    worksheet[ks_type].write(row+12,column+2,'=SUM(C4:C'+ str(total_row['size'])+')'+'/1000000000',num_format4)
    worksheet[ks_type].write(row+12,column+3,' in GB',num_format1)


    worksheet[ks_type].write_comment('C3',"A single set of data not to include the replication factor.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('G3',"The "+ks_type_abbr[ks_type]+" table's total read requests based on a read consistancy level (CL) of LOCAL QUORUM and the local DC's replication factor (RF) of 3. If the read CL is set to LOCAL ONE, then the actual value will be up to 2X this number.  The time is determined by the node's uptime.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('H3',"The "+ks_type_abbr[ks_type]+" table's read request per second based on a read consistancy level (CL) of LOCAL QUORUM and the local DC's replication factor (RF) of 3. If the read CL is set to LOCAL ONE, then the actual value will be up to 2X this number.  The time is determined by the node's uptime.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('I3',"The "+ks_type_abbr[ks_type]+" table's read pecentage of the total read requests in the cluster. (See comment in Total Read Req)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('J3',"The "+ks_type_abbr[ks_type]+" table's pecentage of read requests of the total RW requests (read and Write) in the cluster. (See comment in Total Read Req)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('N3',"The number of "+ks_type_abbr[ks_type]+" table's write requests on the coordinator nodes during the nodes uptime, analogous to client writes.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('O3',"The "+ks_type_abbr[ks_type]+" table's write request per second.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('P3',"The "+ks_type_abbr[ks_type]+" table's write pecentage of the total write requests in the cluster. (See comment in Total Read Req)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('Q3',"The "+ks_type_abbr[ks_type]+" table's pecentage of write requests of the total RW requests (read and Write) in the cluster. (See comment in Total Read Req)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S3',"The "+ks_type_abbr[ks_type]+"'s total read requests based on a read consistancy level (CL) of LOCAL QUORUM and the local DC's replication factor (RF) of 3. If the read CL is set to LOCAL ONE, then the actual value will be up to 2X this number.  The time is determined by the node's uptime.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S4',"The "+ks_type_abbr[ks_type]+"'s read request per second based on a read consistancy level (CL) of LOCAL QUORUM and the local DC's replication factor (RF) of 3. If the read CL is set to LOCAL ONE, then the actual value will be up to 2X this number.  The time is determined by the node's uptime.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S5',"The "+ks_type_abbr[ks_type]+"'s read request per month based on a read consistancy level (CL) of LOCAL QUORUM and the local DC's replication factor (RF) of 3. If the read CL is set to LOCAL ONE, then the actual value will be up to 2X this number.  The time is determined by the node's uptime. Month is calcualted as 365.25/12 days.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S6',"The "+ks_type_abbr[ks_type]+"'s pecentage of read requests of the total RW requests (read and Write) in the cluster. (See comment in Read Requests)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S7',"The number of "+ks_type_abbr[ks_type]+"'s write requests on the coordinator nodes during the nodes uptime, analogous to client writes.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S8',"The "+ks_type_abbr[ks_type]+"'s write request per second.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S9',"The "+ks_type_abbr[ks_type]+"'s write request per month. Month is calcualted as 365.25/12 days.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S10',"The "+ks_type_abbr[ks_type]+"'s pecentage of the total write requests of the total RW requests (read and Write) in the cluster. (See comment in Read Requests)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S11',"The number of "+ks_type_abbr[ks_type]+"'s  read and write requests during the nodes uptime. (See comment in Read Requests)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S12',"The "+ks_type_abbr[ks_type]+"'s total transactions per second. (See comment in Read Requests)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S13',"The "+ks_type_abbr[ks_type]+"'s total transactions per month. Month is calcualted as 365.25/12 days. (See comment in Read Requests)",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})
    worksheet[ks_type].write_comment('S14',"A "+ks_type_abbr[ks_type]+" total data set  size not to include the replication factor.",{'visible':0,'font_size': 12,'x_scale': 2,'y_scale': 2})


  workbook.close()
  print('"' + cluster_name + '_' + 'workload' + '.xlsx"' + ' was created in "' + cluster_url) +'"'
exit();


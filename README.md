# Cassandra_Workload_Extractor

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Summary](#summary)
* [Script Origins](#origins-of-the-code)
* [Getting Started](#getting-started)
* [Using the Cluster Load Spreadsheet](#using-the-cluster-load-spreadsheet)

<!-- SUMMARY -->
## Summary

This script was designed to give insight on a Cassandra Database by identifying primary application load tables with read/write totals and percentages of total RW traffic based on data from the log files.  It produces a Excel spreadsheet with read and write traffic totals for application tables in the Cassandra cluster. It also includes tps data for each table. 

This script gives cluster transaction totals including TPS,TPD and TPMO-Transactions per Month (356/12).  It also includes total log time (including all nodes).

<!-- ORIGINS OF THE CODE -->
## Origins of the Code
This code was created to assist in identifying average tps numbers.  For so long, the max tps ruled everything.  Environments were built on the daily, weekly or monthly max loads.  Now that there is a Cassandra DBaaS - DataStax Astra (https://astra.datastax.com) with prices based on averages, the nessesity to get average transaction numbers is important. Enjoy!! 

<!-- GETTING STARTED -->
## Getting Started

After cloning the Cassandra Wokload Extractor project, download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter. 

If you do not have DSE Opscenter collect the following files and add the in the file structure below.
Run the following nodetool commands on each node
 - nodetool cfstats > cfstats
 - nodetool info > info
 - nodetool describecluster > describecluster

```
[Cluster_Name]
  nodes
    [ipaddress]
      nodetool
        cfsats
        info
        describecluster
```

### Commands and Arguments

#### Creating the Cluster Load Spreadsheet
To create the 
the cluster load data spreadsheet run the following command:
```
python extract_load.py -p [path_to_diag_folder]
```
You may run the script on multiple diagnostic folders:
```
python extract_load.py -p [path_to_diag_folder1] -p [path_to_diag_folder2] -p [path_to_diag_folder3]
```
You may limit the load to just the top n% of the reads and/or writes:
'''
python extract_load.py -p [path_to_diag_folder] -rt 85 -rt 85
'''
You may add the system workload.  This workload remians seperated from the appplication workload by using spreadsheet tabs:
'''
python extract_load.py -p [path_to_diag_folder] -sys
'''

#### Help
There is a brief help info section:
```
python extract_load.py --help
``` 

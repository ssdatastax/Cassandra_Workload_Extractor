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

Install XlsxWriter and Pandas

After cloning this project, download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter or using the Cassandra Diagnostic Collection Tool - https://github.com/datastax-toolkit/diagnostic-collection. 

Note:  If you are using the Cassandra Diagnostic Collection tool, it is easiest to collect a complete cluster diag tarball at once using: 
```sh
./collect_diag.sh -t dse -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
or for open source cassandra:
```sh
./collect_diag.sh -t oss -f mhosts -r -s \
  "-i ~/.ssh/private_key -o StrictHostKeyChecking=no -o User=automaton"
```
mhost is a file with a list of nodes (one per line)

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
You may limit the load to just the top n% of the reads and/or writes (read and write thresholds):
```
python extract_load.py -p [path_to_diag_folder] -rt 85 -wt 85
```
You may add the system workload as an addtional tab in the spreadsheet.  The system workload remians seperate from the appplication workload:
```
python extract_load.py -p [path_to_diag_folder] -sys
```

#### Help
There is a brief help info section:
```
python extract_load.py --help
``` 

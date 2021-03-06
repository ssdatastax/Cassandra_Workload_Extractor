# Cassandra_Workload_Extractor

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [Summary](#summary)
* [Getting Started](#getting-started)
* [Using the Cluster Load Spreadsheet](#using-the-cluster-load-spreadsheet)
* [NoSQLBench YAML Files](#nosqlbench-yaml-files)
* [Collecting Diagnostic Data](#collecting-diagnostic-data)
* [Acknowledgements](#acknowledgements)

<!-- SUMMARY -->
## Summary

This script was designed to give insight on a Cassandra Database by identifying primary application load tables with read/write totals and percentages of total RW traffic based on data from the log files.  It produces a Excel spreadsheet with read and write traffic totals for application tables in the Cassandra cluster. It also includes tps data for each table. 
This script gives cluster transaction totals including TPS,TPD and TPMO-Transactions per Month (356/12).  It also includes total log time (including all nodes).

<!-- GETTING STARTED -->
## Getting Started

After cloning the Cassandra Wokload Extractor project, download a diagnostic tarball from a targeted Cassandra cluster through DSE OpsCenter. 

If you do not have DSE Opscenter collect the following files and add the in the file structure below.
Run the following nodetool commands on each node
 - nodetool cfstats
 - nodetool info
 - nodetool describecluster

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

#### Help
There is a brief help info section:
```
python extract_load.py --help
```

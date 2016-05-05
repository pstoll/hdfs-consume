HDFS Consume
============

The script recursively scans hdfs directories with "hdfs dfs -du" command (you can change this) and prints the list of directories bigger then some threshold (300G by default)

Example run:
------------
```
python hdfs-consume.py -i / > consume.out
```
Output:
```
311.38T   /dir1/subdir1/subdir21/subdir111
134.56T   /dir2/subdir2/subdir33/file1.db
125.39T   /dir1/subdir1/subdir22/subdir122
91.57T    /dir2/subdir3/subdir33/file2.db
89.6T     /dir1/subdir1/subdir53/subdir312
78.49T    /dir1/subdir1/subdir31/subdir521
77.85T    /dir1/subdir3/subdir21/subdir192
```

Arguments:
----------
|                  Using             |             Description           |      Default      |Required|
|------------------------------------|-----------------------------------|-------------------|--------|
| --threshold THRESHOLD, -t THRESHOLD|Minimum dir size to show (in bytes)|322122547200 (300G)|no|
|--depth DEPTH, -d DEPTH             |Max directory level to scan        |3                  |no
|--log FILENAME, -l FILENAME         |Logfile name                       |/tmp/hdfs-consume.log|no|
|--verbosity, -v                     |Logging level, use -vvvv to debug  |1 (ERROR)          | no|
|--cmd CMD                           |Command string for running "hdfs -du" as subprocess|"sudo -u hdfs hdfs dfs -du"|no|
|--interactive, -i                   |Print scanning progress            |False | no
|path                                |Path to start scanning             |'/'   | yes

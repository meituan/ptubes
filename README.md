
## Introduction
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

___
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_CN.md)

___

Ptubes is a database disaster recovery product based on PITR (Point In Time Recovery), which can be used to restore the entire database to a specific point in time to help users improve the reliability and security of the database. The product consists of three core components, Reader, Storage and SDK. It provides functions such as safe backup and efficient distribution of database change events. Typical scenarios are as follows:
* data backup
* Data playback
* Data Recovery
* Event driven
* The database is more active

## Quick Start

### Prerequisite

* 64bit JDK 1.8+

* Maven 3.2.x

### Run Reader

**1.mysql preparation**<br>
Ptubes synchronization data requires mysql to support Binlog ROW mode in advance, users need to modify the binlog mode in advance
```
[mysqld]

log-bin=mysql-bin # Open binlog

binlog-format=ROW # Modify to ROW mode
```
**2.Start Reader**<br>
2.1.Download the compressed package<br>
[ptubes-reader-server.tar.gz](https://github.com/meituan/ptubes/releases/latest)<br>

2.2.Unzip to any directory
```
mkdir /user/ptubes
tar zxvf ptubes-reader-server.tar.gz -C /tmp/ptubes
```
After decompression, you can see the directory structure
```
drwxr-xr-x   4 yangmouren  staff   128  2 17 16:47 bin
drwxr-xr-x   5 yangmouren  staff   160  2 17 16:54 conf
drwxr-xr-x  63 yangmouren  staff  2016  2 17 17:00 lib
```

2.3.Modify related configuration information
In the conf directory, modify a reader.conf and fill in the configuration
```ReaderServer.conf
ptubes.server.tasks=demoR1,demoR2 //Task name, each task needs to have configuration information of the corresponding file, separated by commas
```
The program will continue to read the ReaderTask related configuration according to the parameters of ptubes.server.tasks. Take the above picture as an example, the program will automatically search for demoR1.properties and demoR2.properties from the current directory, and we will create corresponding files respectively (as shown in the following figure)
```demoRx.properties
ptubes.reader.mysql.host= //mysql host address
ptubes.reader.mysql.port= //mysql port
ptubes.reader.mysql.user= //mysql username
ptubes.reader.mysql.passwd= //mysql password
```
Then run the program to start the ptubes exploration journey.

2.4.Start the service
```
sh bin/start.sh
```
2.5.View service log
```
tail -f logs/reader.log
```
2.6.Shut down the service
```
sh bin/stop.sh
```
### Start the SDK
[SDK start](https://github.com/meituan/ptubes/wiki/%E5%AE%A2%E6%88%B7%E7%AB%AF%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97)
### more configuration
[configuration document](https://github.com/meituan/ptubes/wiki/%E9%85%8D%E7%BD%AE%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97)

## Documentation
- 1.[Home](https://github.com/meituan/ptubes/wiki)
- 2.[Architecture-design](https://github.com/meituan/ptubes/wiki/Architecture-design)
  - 2.1.[Overall Architecture and Deployment](https://github.com/meituan/ptubes/wiki/Architecture-design#1-overall-architecture-and-deployment)
  - 2.2.[Module description](https://github.com/meituan/ptubes/wiki/Architecture-design#2-module-description)
  - 2.3.[implementation details](https://github.com/meituan/ptubes/wiki/Architecture-design#3-implementation-details)
- 3.[QuickStart](https://github.com/meituan/ptubes/wiki/QuickStart)
  - 3.1.[mysql-preparation](https://github.com/meituan/ptubes/wiki/QuickStart#2mysql-preparation)
  - 3.2.[Start Reader](https://github.com/meituan/ptubes/wiki/QuickStart#3-start-reader)
- 4.[Client User Guide](https://github.com/meituan/ptubes/wiki/Client-User-Guide)
  - 4.1.[Start mode one](https://github.com/meituan/ptubes/wiki/Client-User-Guide#2-start-mode-one)
  - 4.2.[Start mode two](https://github.com/meituan/ptubes/wiki/Client-User-Guide#3-start-mode-two)
  - 4.3.[Start mode three](https://github.com/meituan/ptubes/wiki/Client-User-Guide#4-start-mode-three)
- 5.[Configuration usage guide](https://github.com/meituan/ptubes/wiki/Configuration-usage-guide)
  - 5.1.[Reader Configuration](https://github.com/meituan/ptubes/wiki/Configuration-usage-guide#1-reader-configuration)
  - 5.2.[SDK Configuration](https://github.com/meituan/ptubes/wiki/Configuration-usage-guide#2sdk-configuration)
- 6.[Local Debugging Guide](https://github.com/meituan/ptubes/wiki/Local-debugging-guide)

## License
[Apache License, Version 2.0](LICENSE) Copyright (C) Apache Software Foundation

## issues
[Ptubes ISSUES](https://github.com/meituan/ptubes/issues)

## Contact us
![Contact us](https://raw.githubusercontent.com/wiki/meituan/ptubes/images/lianxifangshi.jpeg)

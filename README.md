# Light-Weight-MapReduce
Light Weight MapReduce is a heterogeneous platform to perform computationally expensive tasks collaboratively.

Setup Guide:

	1.	Change the IP address of host computer in Data Nodes and Task Tracker.
	2.	Create a folder named “nameNodeDB” in the home folder.
	3.	Clear for the first time “FilesDB.xml” and place in this folder.
	4.	Start the NameNode.java
	5.	Now connect the DataNodes by either running DataNodesComp.java (Computer Version) or DataNodesAnd.java (Android Version).
	6.	Once connected..everytime one device connects “accepted 1, 2, ...” will be shown.
	7.	Now push the file “push Muneeb.txt Dirx/ “Replication level”__Upper-Bound-Total-File-Block-Size-In-Bytes”
	8.	e.g. 4 Data Nodes with a total file of 100 mb to process and replication level 2. “push Muneeb.txt Dirx/ 2  25000000” {There are 2 spaces after replication}
	9.	Once the file is pushed. Start the “JobTracker.java”
	10.	Start either “TaskTrackerComp.java” or “TaskTrackerAnd”.
	11.	Everytime you the task tracker is accepted it will be written on console.
	12.	Now give the job. “job PlainApp PlainApp Dirx/Muneeb.txt”
	13.	You can check MDFS folder in the SDCARD to check the mapper splits and mapper output.

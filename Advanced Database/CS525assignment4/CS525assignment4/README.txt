---Assignment 3 --- Record Manager---

The purpose of this assignment is  creating a record manager. The record manager handles tables with a fixed schema.
Clients can insert records, delete records, update records, and scan through the records in a table. A scan is associated
with a search condition and only returns records that match the search condition.

We implement the tombstone part for bonus.


---Environment---
Mac OS


---Compiler---
X code


---Functions---

---Table and Record Manager Functions

Struct tableInfo: It defines the information about the table, such as schemalength, schemapages, startpage, lastpage, number of tuples, record size and maxrecords.
Struct scanInfo: scanID and total number of records are defined in it.


RC initRecordManager 
RC shutdownRecordManager

RC createTable: 
   First, checking whether the table exists or not.
   Then creating a file with given name and create pages for info and schema.
   The first page used for file info, second page used for schema.

RC openTable: 
   First, checking whether the table exists or not. 
   Then it will initializes the buffermanager. 
   Finally, loading the page, schema and tableinfo.

RC closeTable:
   Shut down the bufferpool and free the schema.

RC deleteTable:
   Checking whether the table exist or not by given name. If it is not exist, returning the RC_TABLE_NOT_FOUND.
   Then, removing the table.

int getNumTuples:
   Getting the number of tuples from the tableInfo.


---Record Functions

RC insertRecord:
   Getting the page number and slot, storing in the record and info.
   Then, do the pinpage, markdirty, forcepage process for updating.
   Writing results to table.

RC deleteRecord:
   We use the symbol "@@" for tombstone.

RC updateRecord:
   Using pinPage, markDirty, unpinPage, and forcePage functions for updating.

RC getRecord:
   Using pinPage and unpinPage for updating.
   Check the tombstone.
   Then get the page number and slot.


---Scan Functions

RC startScan:
   Initialize the RM_ScanHandle data structure.
   Assign sinfo to scan->mgmtData.

RC next:
   Fetch the record as per the page and slot id.
   Evaluate the given expression to check if the record is the one required by the client.
   If the record fetched is not the required one then call the next function with the updated record id parameters.

RC closeScan


---Schema Functions

int getRecordSize:
   Get the size of the record based on the schema, also considering the different types.

Schema *createSchema:
   Get numAttr, attrNames, dataTypes, typeLength, keySize and keys into schema.

RC freeSchema


---Attribute Functions

RC getAttr£º
   Allocate the space to the value data structre where the attribute values are to be fetched. 
   Get the offset value of different attributes as per the attribute numbers.
   Attribute data is assigned to the value pointer as per the different data types.

RC setAttr:
   Get the offset value of different attributes as per the attribute numbers.
   Set attribute values as per the attributes datatype. 


--- Team member---
YIZHI GU A20334298
SICHAO LI A20337719
JIE HUANG A20279698

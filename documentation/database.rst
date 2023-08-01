Database. Collections. Data storage.
========================================

The database is a directory with *.db collection files (text files) in which the data is stored in a special JSON-similar structure

Connecting to the database:

.. code-block:: Python
  
     db = SimpleBase("database_name",path="path to database",timeout=<timeout>)

The database name is the name of the directory where the data is stored. Path (optional) – path to the base directory. Timeout (optional) – the maximum allowable table lock wait time in seconds during multi-user work. The default is 60 seconds.

All data is in collections. Each collection is a separate file on disk. In this case, the collection data is always in RAM in a dictionary, but at each access it is checked if there are any changes in the data by another user/thread. 

At the same time, when performing any data change operation, the data is immediately written to the files, while the change identifier is updated (by which the need to reread the data from disk is tracked)

The collection is specified like this:

.. code-block:: Python
      
     db[<collection name>]

Since the collection must be read at least once, and this can be a time-consuming process, you can read it in advance, for example, with the following command:

.. code-block:: Python
     
     db[<collection name>].get(“”)

If the collection is not yet in memory, then it will be read at any time it is accessed.

To read all database collections there is an initialize() command

.. code-block:: Python
     
     db.initialize()

Transactions
-----------------

Several operations can be combined into one transaction so that the logical execution of all operations ensures the integrity of the entire transaction. All operations in the transaction are performed in memory, and at the exit from the transaction are recorded in files.

Transaction example

.. code-block:: Python

     with DBSession(db) as s:
          inserted = db['income'].insert({"product_id":id} , session=s)
          inserted = db['outgoing'].insert({"product_id":id} , session=s)

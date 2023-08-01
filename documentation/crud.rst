CRUD operations
==================

Insert/Upsert
----------------

You can add a new entry without an ID, then the ID will be assigned automatically.

.. code-block:: Python
  
  id = db['goods'].insert({"name":"coffee", "price":15})

In this database, id is stored in documents in the **_id** field

You can specify _id in the document

But if such _id already exists, an error will be generated.

.. code-block:: Python
  
  id = db['goods'].insert({"name":"coffee", "price":15, "_id”: "1"})
  
If it is necessary to execute "add entry, if there is - update", i.e. “upsert” then you must specify the key **upsert = True**

.. code-block:: Python
                            
  id = db['goods'].insert({"name":"coffee", "price":15, "_id”: "1"}, upsert=True)

It is also possible to specify the no_index =True parameter in order to disable updating indexes during collection operations (this applies to all operations)

An insert can be either one record or a set. Instead of insert and insert_many, just use insert with a list as a parameter

Updating Records
-------------------
The **update** command is used to update existing records (unlike insert with the upsert=True key) it only updates the records that are in the database

.. code-block:: Python

  collection.update((<ID>|[BL list]|{<condition>},<values to update>)
  
The command updates one document - ID (string), several documents - ID (list) or selection by condition (dictionary)

In this case, you can specify as element IDs (one or a list):

.. code-block:: Python

  db['new_goods5'].update(["7fe24b01-46f1-4b06-95f7-4addceb21fdb","faa6200e-aaae-4e23-a7f3-155f3301b597"],{"done":True})
  
 , and the condition:

.. code-block:: Python
                          
  res = db['goods'].update({"name":"apple"},{"find":True,"name":"Apple"})

Removing documents
--------------------
                          
A command **delete**

.. code-block:: Python
  collection.delete(<id>|[BL list]|{<condition>})
                          
removes a single ID (string), multiple IDs (list), or a selection by condition (dictionary)

.. code-block:: Python
                          
  db['test'].delete('222')
                          
Cleaning up the collection
----------------------------

.. code-block:: Python
                         
  collection.clear()

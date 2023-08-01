Indexes and Subscriptions
===============================

Hash index
-------------

The hash index is a dictionary of the field's hash values. Its filling (re-indexing) requires a certain time, but searching through the index is almost instantaneous. It is intended to be used only for unique values. At least it will always return only one document.

To register an index or change conditions, call **register_hash_index** function once. For example:

.. code-block:: Python
  
    register_hash_index(<index name>,<field name>, <additional argument>) 

For example:

.. code-block:: Python
                        
  db['query_test'].register_hash_index("my_hash_index","name",dynamic = True)
                        
where the **dynamic=True** flag indicates that the index will not be stored on disk, it will only exist in memory. Those. In order to use the index, you need to re-index
                        
After registering an index, all changes to the collection included in the index (insert, update, delete) will lead to a change in the index. This should be taken into account to understand the performance of operations. To disable such movements, you can specify the no_index=True flag in the arguments

You can also force re-index the entire index with the command

.. code-block:: Python
                        
    collection.reindex_hash(<index name>)
                            
For example,

.. code-block:: Python
                            
    db['query_test''].reindex_hash("my_hash_index")

Searching by index is actually getting the value by the key in the dictionary and is performed by the get_by_index(<index>,<value>) function
                                                                                                                   
For example,
                                                                                                                   
.. code-block:: Python
                                                                                                                   
  r = db['query_test'].get_by_index(db["my_hash_index "],"apple")


Text index
-----------------
                                                                                                                   
Required to significantly speed up the search for a substring in a string. It is a variation of a balanced binary tree.
                                                                                                                   
To register an index or change conditions, call the **register_text_index** function once. For example:

.. code-block:: Python
                                                                                                                   
    register_text_index(<index name>,<field name>, <additional argument>)                                                                                                                   
                                                                                                                   
db['query_test']. register_text_index("my_text_index","name",dynamic = True)
                                                                                                                   
.. note::  As with hash indexes, registration leads to the fact that all operations begin to affect the index, but the generation time is somewhat longer, and the volume of files is quite significant. So it is recommended to keep it in memory ( dynamic = True)
                                                                                                                   
You can also force re-index the entire index with the command

.. code-block:: Python
                                                                                                                   
  collection.reindex_text(<index name>)

For example,

.. code-block:: Python
                        
    db['query_test''].reindex_text("my_text_index")

To search by index, use the function

.. code-block:: Python
                          
    index. search_text_index(<search substring>)
                             
For example:

.. code-block:: Python
                             
    db['goods'].search_text_index("appl")

Subscriptions
------------------                             
                             
A subscription is a collection that automatically registers all changes to the selected collections. That is, during any operations, the IDs of the documents for which the data was changed will be added to this collection
Subscriptions are registered by command:

.. code-block:: Python
                             
    Database.create_subscription(<subscription name>,[<list of collection names>])
                                                      
For example

.. code-block:: Python
                                                      
  db.create_subscription("my_subscription",['income','operations'])
                                                      
A subscription is a regular collection, so you can get data by any function for getting data - all, find, etc.

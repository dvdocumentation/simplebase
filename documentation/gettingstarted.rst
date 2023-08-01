Basic usage
====================

Installation

.. code-block:: Python

  pip install pysimplebase

First, let's create a database object

.. code-block:: Python
  
  from simplebase import SimpleBase,DBSession
  
  db = SimpleBase("samples_db")

Insertion, like other CRUD commands, can be for one record and for several. Recognition is automatic depending on the type of the argument

.. code-block:: Python
  
  id = db['goods'].insert({"name":"coffee", "price":15}) #insert one document
  inserted = db['goods'].insert([{"name":"apple", "price":2},{"name":"apple", "price":3}]) #insert dataset

The result of insert, like any CRUD commands, is one or more document IDs

Сan work in a transaction if you need to maintain the integrity of the operation

.. code-block:: Python

  with DBSession(db) as s:
      inserted = db['income'].insert({"product_id":id} , session=s)
      inserted = db['outgoing'].insert({"product_id":id} , session=s)

Some more CRUD operations

.. code-block:: Python

  #updating
  db['goods'].update(inserted,{"updated":True})

  #deleting
  db['goods'].delete(id)
  db['goods'].delete(inserted)

  #clear
  db['goods'].clear()

Data search, queries
``````````````````````

Just get by id:

.. code-block:: Python

  db['goods'].get(id)

Select all documents in collection

.. code-block:: Python

  db['goods'].all()

Simple field match search

.. code-block:: Python

  result = db['goods'].find({"name":"apple"})

Query with logical operators

.. code-block:: Python

  result = db['goods'].find({"$and":[
    {"price":{"$gt":1}},
    {"price":{"$lte":10}}
    ]}
    )

For large selections, indexes should be used. Indexes can be disk-based and dynamic. Both are built into CRUD operations and are updated when records are updated (this can be disabled) and can be re-indexed (optional)
Hash indexes are a dictionary of hash values of the searched field, i.e. getting values at such an index is almost instantaneous, unlike any other search

.. code-block:: Python

  db['goods'].register_hash_index("hash_dynamic","name", dynamic=True) #there are dynamic and stored indexes
  db['goods'].reindex_hash("hash_dynamic")
  r = db['goods'].get_by_index(db["hash_dynamic"],"apple")

Text indexes are needed to quickly find documents in which a certain field contains a substring

.. code-block:: Python

  
  db['goods'].register_text_index("fts","name", dynamic=True) #there are dynamic and stored indexes
  db['goods'].reindex_text("fts")
  r = db['goods'].search_text_index("appl")

SimpleBase is a JSON based serverless database with improved performance on key operations

The strength of document-oriented NoSQL DBMS is their natural simplicity, but they are usually not very fast (unless they are serious server DBMS like MongoDB). SimpleBase fixes performance issues in critical areas

 * **Instant addition** of new records to collections, regardless of the size of the collection due to a special storage architecture
 * Faster work with operations, due to the fact that it is **not required to encode / decode the entire collection** (which can be very large)
 * Collections are always **stored in RAM** with concurrency change tracking: data is re-read from disk only if it has been changed by another process
 * **ACID for multi-user and multi-threading**
 * Two types of indexes for key types of queries - a **hash index** and a **special B-tree** for full-text search
 * Support for **transactions** (sessions)
 * MongoDB-like syntax, incl. 100% similar query language
 * Written in **pure python**, only about 1800 lines

Why SimpleBase?
------------------

It was written for situations where you need to organize a local database without a server with a JSON-oriented interface. But at the same time, increased performance requirements: for large collections (1000000+ documents in the collection), fast, almost instantaneous execution of some operations is required:

• Add a new document to the collection with 1000000 documents - 0.007 seconds, with 2000000 documents - also 0.0007 seconds - ie. operation time does not depend on the size of the table
• Find an element by equality in a collection with 1000000+ entries in 1-2 microseconds
• Organize real-time search by occurrence of a string across a large collection without friezes

Getting started sapmle
--------------------------

.. code-block:: Python

  from simplebase import SimpleBase,DBSession
  
  #creatig database
  db = SimpleBase("samples_db")
  
  #inserting documents into collection
  id = db['goods'].insert({"name":"coffee", "price":15}) #insert one document
  print(db['goods'].get(id))
  
  inserted = db['goods'].insert([{"name":"apple", "price":2},{"name":"apple", "price":3}]) #insert dataset
  
  #insert or update (upsert)
  db['goods'].insert({"name":"coffee", "price":16,"_id":id},upsert=True)
  print(db['goods'].get(id))
  
  #transaction
  with DBSession(db) as s:
      inserted = db['income'].insert({"product_id":id} , session=s)
      inserted = db['outgoing'].insert({"product_id":id} , session=s)
  
  #updating
  db['goods'].update(inserted,{"updated":True})
  print(db['goods'].all())
  
  #simple search without index
  result = db['goods'].find({"name":"apple"})
  
  #building complex queries
  result = db['goods'].find({"$and":[
      {"price":{"$gt":1}},
      {"price":{"$lte":10}}
      ]}
      )
  print(result)
  
  #hash indexes for unique values
  db['goods'].register_hash_index("hash_dynamic","name", dynamic=True) #there are dynamic and stored indexes
  db['goods'].reindex_hash("hash_dynamic")
  r = db['goods'].get_by_index(db["hash_dynamic"],"apple")
  
  
  #text indexes
  db['goods'].register_text_index("fts","name", dynamic=True) #there are dynamic and stored indexes
  db['goods'].reindex_text("fts")
  r = db['goods'].search_text_index("appl")
  
  #delete
  db['goods'].delete(id)
  db['goods'].delete(inserted)
  
  #clear
  db['goods'].clear()

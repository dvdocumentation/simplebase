Queries.
=============

Get all data from the collection (returns list of a document):

.. code-block:: Python

  collection.all()

Get one document by ID (returns one record):

.. code-block:: Python
 
  collection.get(<ID>)
                 
 This is a fast command. the call is always made to the dictionary in memory.
                 
Simple search
--------------------

.. code-block:: Python
 
    Collection.find({<search pattern>})
                 
Simple search example (will find all documents that have a name field and it is equal to apple):

.. code-block:: Python
                
    result = db['goods'].find({"name":"apple"})

Queries
--------------
                 
The query syntax is partially equivalent to MongoDB. All queries are executed by the find function with a condition, which can be multipart and include other conditions.
Syntax (execution result - list of documents):

.. code-block:: Python
                 
  collection.find({<field>:{<operator>:<comparison value>}})
  
For this, comparison operators are used:
                 
 * **$eq** is equivalent to **=**, matches values that are equal to a specified value.
 * **$gt** is equivalent to **>**, matches values that are greater than a specified value.
 * **$gte** is the equivalent of **>=**, matches values that are greater than or equal to a specified value.
 * **$in** - matches any of the values specified in an list.
 * **$lt** - equivalent to **<**, matches values that are less than a specified value.
 * **$lte** - equivalent to **<=**, matches values that are less than or equal to a specified value.
 * **$ne** is equivalent to **!=**, matches all values that are not equal to a specified value.
 * **$regex** - execution of a regular expression expression, to check the condition on a line of text

And logical operators:
                 
 * **$and** - joins query clauses with a logical AND returns all documents that match the conditions of both clauses.
 * **$not** - inverts the effect of a query expression and returns documents that do not match the query expression.
 * **$or** - joins query clauses with a logical OR returns all documents that match the conditions of either clause.

An example of two comparison operations combined with the $and operator:

.. code-block:: Python
 
      result = db['goods'].find({"$and":[
     {"price":{"$gt":1}},
     {"price":{"$lte":10}}
     ]}
     )

Another example with the $not operator. The note operator is particularly needed in cases where the field being tested is not present in all fields.

For example:

.. code-block:: Python
                 
  db['logic'].insert({"nom":"apple","qty":5,"done":True})
  db['logic'].insert({"nom":"banana","qty":7})
  db['logic'].insert({"nom":"cherry","qty":15})

  res = db['logic'].find( { "done":{"$not" : { "$eq": True } }} )

this expression will return 2 documents - *banana* and *cherry*

and this expression will return an empty list:


.. code-block:: Python

  res = db['logic'].find( { "done": { "$ne": True } } )

Examples with regex

.. code-block:: Python

  res = db['goods'].find({"name":{"$regex":"appl"}})

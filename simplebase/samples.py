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

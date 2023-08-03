from typing import Dict, Iterator, Protocol, Set, Type,Callable,Mapping,Optional, NoReturn
import uuid
import os
import json
from pathlib import Path
from filelock import Timeout, SoftFileLock
from typing import Any,List
from types import TracebackType
import time
import hashlib
import re
from msgspec.json import decode,encode

import copy
from typing import List
import itertools
import io
import fileinput
import os

import mmap

#default timeout value
LOCK_TIMEOUT = 90

#B-tree branches sise for text search
BRANCH_SIZE=10



# Utilities

"""
General encoding and decoding functions. 
If necessary, it can be replaced by json.dumps() and json.loads()
But msgspec is faster!

"""
def to_json_str(value):
     return encode(value).decode()

def from_json_str(value):
     return decode(value)

"""
condition check function
Syntax replicates MongoDB query syntas

:condition: query condition
:document: document

:returns: True or False condition is met to the document


"""
def check_condition(condition, document):
    if isinstance(condition,Dict):
        for key,value in condition.items(): #logical expression AND and OR
            if isinstance(value,List):
                if key=="$and":
                    res = None
                    for element in value:
                        if res==None:
                            res = check_condition(element, document)
                        else:    
                            res=res and check_condition(element, document)
                    return res 
                elif key=="$or":
                    res = None
                    for element in value:
                        if res==None:
                            res = check_condition(element, document)
                        else:    
                            res=res or check_condition(element, document)
                    return res  
                
            else:    
                if isinstance(value,Dict): #comparison operators
                    
                    if '$regex' in value: #regular expression matching
                        for key_document,value_document in document.items():
                            if key==key_document and isinstance(value_document,str):
                                pattern = re.compile(value['$regex'])
                            
                                if pattern.search(value_document):
                                    return True
                                else:
                                    return False
                    elif '$ne' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value['$ne']!=value_document:
                                    return True
                                else:
                                    return False
                    elif '$not' in value:
                         return not check_condition({key:value["$not"]}, document)           
                    elif '$in' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                                if value_document in value['$in']:
                                    return True
                                else:
                                    return False 
                    elif '$nin' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                                if not value_document in value['$in']:
                                    return True
                                else:
                                    return False                        
                    elif '$eq' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value['$eq']==value_document:
                                    return True
                                else:
                                    return False 
                    elif '$gt' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document>value['$gt']:
                                    return True
                                else:
                                    return False
                    elif '$gte' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document>=value['$gte']:
                                    return True
                                else:
                                    return False   
                    elif '$lt' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document<value['$lt']:
                                    return True
                                else:
                                    return False
                    elif '$lte' in value:
                        for key_document,value_document in document.items():
                            if key==key_document:
                        
                                if value_document<=value['$lte']:
                                    return True
                                else:
                                    return False
                                                                                        
                else:   #simple condition (pattern search)
                    for key_document,value_document in document.items():
                        if key==key_document:
                            if value==value_document:
                                return True
                            else:
                                return False
    return False

def extract_dict_string(textfile,r,searchstr):
    count_br=1
    count_quotes=0
    start = r+searchstr.find("{")
    jbody =textfile[start:start+1]
    current = start+1
    while count_br>0 :
        current_symbol = textfile[current:current+1]
        prev_symbol = textfile[current-1:current]
        if current_symbol == "{" and count_quotes==0:
            count_br+=1
        elif current_symbol == "}" and count_quotes==0:
            count_br-=1  
        elif current_symbol == '"' and prev_symbol!='\\':
            count_quotes+=1 
            if count_quotes>1:
                count_quotes=0     

        jbody+=current_symbol
        current+=1  

    return jbody

"""
splits the dictionary into 2 dictionaries
"""
def splitDict(d):
    n = len(d) // 2          
    i = iter(d.items())      

    d1 = dict(itertools.islice(i, n))   
    d2 = dict(i)                        

    return d1, d2

"""
This function builds a special balanced binary tree.
Each branch concatenates all values of the indexed field that is being searched.
Thus, the search includes branches containing the desired substring
"""
def split_list2(base):
            slist ={}
            if len(base)>BRANCH_SIZE:
                splitted = splitDict(base)
                slist["0"]={}
                slist["0"]["_id"] = "0"
                slist["0"]["base"] = splitted[0]
                slist["0"]["ids"] = list(slist["0"]["base"].keys())
                slist["0"]["text"] = "".join(list(slist["0"]["base"].values()))

                slist["1"]={}
                slist["1"]["_id"] = "1"
                slist["1"]["base"] = splitted[1]
                slist["1"]["ids"] = list(slist["1"]["base"].keys())
                slist["1"]["text"] = "".join(list(slist["1"]["base"].values()))
            else:
                slist["0"]={}
                slist["0"]["_id"] = "0"
                slist["0"]["base"] = base
                slist["0"]["ids"] = list(slist["0"]["base"].keys())
                slist["0"]["text"] = "".join(list(slist["0"]["base"].values()))    
                 
            
            if len(slist["0"]["base"])>BRANCH_SIZE:
                for k in slist.keys():
                    current_part = slist[k]
                    current_part['child'] = split_list2( current_part['base'])
                    

            return slist
"""
search for document IDs, along the branches of the B-tree

:index: text index
:s: search string

:returns: list of document IDs

"""
def get_index_ids_by_string(index,s):
    result = []
    for k,v in index.items():
        if s in v["text"]:
            if 'child' in v:
                internal = get_index_ids_by_string(v['child'],s)
            else:
                internal = v['ids']

            for i in internal:
                 result.append(i)
    return result 

"""
Adding a value to B-tree
Balancing happens automatically

:base: dictionary of B-tree values
:value_id: ID to be insert
:value_id: value to be insert

"""
def insert_in_branchces_dynamic_text_binary(base,value_id,value):

    if not '0' in base:
                    base["0"] = {}
                    base["0"]['_id']="0"
                    base["0"]['ids']=[value_id]
                    base["0"]['text']=value
                    base["0"]['base']={value_id:value}
    elif not '1' in base:
        base["1"] = {}
        base["1"]['_id']="1"
        base["1"]['ids']=[value_id]
        base["1"]['text']=value
        base["1"]['base']={value_id:value}

    else:
            if len(base["0"]['ids'])<len(base["1"]['ids']):
                    
                    current_base = base["0"]
            else:     
    
                    current_base = base["1"]
                 
            current_base['ids'].append(value_id)
            current_base['text']+=value
            current_base['base'][value_id] =value 

            if len(current_base['ids'])>BRANCH_SIZE:
                if not 'child' in current_base:
                    current_base['child'] = split_list2( current_base['base']) 
                else:
                    insert_in_branchces_dynamic_text_binary(current_base['child'],value_id,value)                        

"""
Deleting a value from B-tree

:base: dictionary of B-tree values
:value_id: ID to delete
:value_id: value to be removed

"""
def delete_in_branchces_dynamic_text_binary(base,value_id,value):

    for k in base:
        index = base[k]
        if value_id in index['ids']:
            index['ids'].remove(value_id)
            index['base'].pop(value_id)
            index['text']  = "".join(list(index['base'].values()))

            if 'child' in index:
                delete_in_branchces_dynamic_text_binary(index['child'],value_id,value) 

"""
A quick function to write to a file to replace a document with another value.
search is performed by document ID

:filename:  path to collection file
:pattern:  document ID
:replace:  JSON-string representing a document

"""
def replace_value_in_file(filename,pattern,replace):
    # меняем слово
    text = pattern.encode('utf-8')
    # на слово
    repl_text = replace.encode('utf-8')

    with open(filename, "r+b") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_WRITE) as mm:
            current_size = mm.size()
            index_text = mm.find(text)
            index_text_end = mm.find("\n".encode('utf-8'),index_text)+1
            len_text = index_text_end-index_text
            index_data = index_text + len_text
            data = mm[index_data:]
            diff = len(repl_text) - len_text
            mm.resize(current_size + diff)
            mm.seek(index_text)
            mm.write(repl_text + data)
            mm.flush()

"""
Quickly writes the data modification prefix to the collection file

:path: filename of collection
:prefix: modification prefix

"""
def write_prefix(path,prefix):
    with open(path,"r+", encoding='utf-8') as f:
            text =f.read(36)
            f.seek(0)
            f.write(prefix)
"""
Replacing Multiple Values in a File

:filename: filename of collection
:documents: documents for replacement

"""
def replace_values_in_file(filename,documents):
   

    with open(filename, "r+", encoding='utf-8') as fp:
        content = fp.read()
        fp.seek(0) 
        fp.truncate()
        for document in documents:
             pattern = '"'+document.get("_id")+'":'
             ind1 = content.find(pattern)
             if ind1>0:
                  ind2 = content.find("\n",ind1)
                  text = content[ind1:ind2]

                  if "$delete" in document:
                    replace_text=""
                    text = text+"\n"
                  else:  
                    replace_text = pattern+to_json_str(document)
                  
                  content = content.replace(text,replace_text)
                  
        #fp.truncate()
        fp.write(content)          
                  

        
"""
The main class for connecting to the database and working with collections 
"""    
class SimpleBase(dict):
    """
    A class that stores data and in which all basic operations with data and database storage take place
    """
    class Collection:
        #********************** External functions *********************

        # CRUD-functions

        """
        Insert document/documants into collection

        :document: document or list of documents to be added
        
        Arguments:
        :no_index: OFF to change indexes
        :upsert: INSERT or UPDATE
        :session: transaction instance


        """
        def insert(self,document,**kwargs):
            
             if isinstance(document,list):
                  if kwargs.get("session")!=None:
                       result = self.insert_many(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                  else:     
                       result = self.insert_many(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),NoIndex=kwargs.get("no_index"))
             else:   
                  if kwargs.get("session")!=None:
                    result = self.fast_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                  else:  
                    result = self.fast_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"),NoIndex=kwargs.get("no_index"))   
                  #if kwargs.get("fast") == True: 
                  #  result = self.fast_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"))
                  #else:  
                  #  result = self.regular_insert(document,update = kwargs.get("update"),upsert = kwargs.get("upsert"))

             return result 
       
        """
        Update documents by condition
        
        :condition: document ID/ list of documents IDs/ query condition
        :dataset: values to replace

        """
        def update(self,condition,dataset, **kwargs):
             if isinstance(condition,str):
                item = self._data.get(condition) 

                if item == None:
                    return -1
                  
                for key,value in dataset.items():
                    item[key] = value
                  
                if "session" in kwargs:
                    return self.fast_insert(item, update=True, session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))
                else:
                    return self.fast_insert(item, update=True,NoIndex=kwargs.get("no_index"))
             
             elif isinstance(condition,list) or isinstance(condition,dict):
                  set = []
                  if isinstance(condition,list):
                    
                    for itemkey in condition:
                        item = copy.deepcopy(self._data.get(itemkey)) 
                        if item!=None:
                            for key,value in dataset.items():
                                item[key] = value
                            set.append(item)    

                  if isinstance(condition,dict): 
                       result = copy.deepcopy(self.find(condition))
                       for item in result:
                        
                        for key,value in dataset.items():
                            item[key] = value
                        set.append(item) 

                  if "session" in kwargs:

                    return self.insert_many(set, update=True, session = kwargs.get("session"))                     
                  
                  else:
                    
                    return self.insert_many(set, update=True)  
        """
        Delete document/list of document/query results

        :condition: document ID/ list of documents IDs/ query condition

        """
        def delete(self,condition, **kwargs):
             if isinstance(condition,str):
                item = self._data.get(condition) 

                if item == None:
                    return -1
                  
                            
                if "session" in kwargs:
                    return self.fast_delete(item, session = kwargs.get("session"),NoIndex=kwargs.get("no_index"))                     
                else:
                    return self.fast_delete(item,NoIndex=kwargs.get("no_index")) 
             
             elif isinstance(condition,list) or isinstance(condition,dict):
                  set = []
                  if isinstance(condition,list):
                    
                    for itemkey in condition:
                        item = copy.deepcopy(self._data.get(itemkey)) 
                        
                        if item!=None:
                            set.append(item)    

                  if isinstance(condition,dict): 
                       result = copy.deepcopy(self.find(condition))
                       for item in result:
                        
                        if item!=None:
                            set.append(item) 

                  if "session" in kwargs:

                    return self.delete_many(set, session = kwargs.get("session"))                     
                  
                  else:
                    
                    return self.delete_many(set)                                  

        """
        remove all documents
        """
        def clear(self,  **kwargs) -> str:
            self._recording=True
            # Now, we update the table and add the document
            def updater():
                
                
                
                self._data = {}

                return ["$clear"],[],["$clear"]


            if 'session' in kwargs:
                session = kwargs['session']
                res = self._update_collection_memory(updater)
                session._operations[self._name]=res
            else:    
                res = self._update_collection(updater)
                self._recording=False
                if res == -1:
                    raise ValueError('Write failed')
    
        #Query functions

            """
        Return document by ID

        :key: document ID   

        """
        def get(self,key):
            return self._data.get(key)
        
        """
        Return document by hash-value

        :index: hash index cokkection
        :value: value of document

        """
        def get_by_index(self,index,value):
           

            key = index._data.get(hashlib.sha1(value.encode()).hexdigest())
            if key==None:
                return None
            else:
                return self._data.get(key['value'])
        
        """
        Queries documents from the collection by condition

        :condition: query condition
        

        """
        def find(self,condition):    

            data = self._data.copy()
            result = [element for key,element in data.items() if check_condition(condition,element)]


            return result

            # Write the newly updated data back to the storage
            #self._storage.write(tables)

            # Clear the query cache, as the table contents have changed
            #self.clear_cache()
        
        """
        Returns all documents
        """
        def all(self):
             return list(self._data.values())
        
        """
        Pre-reads all the collections
        """
        def initialize(self):
         for r, d, f in os.walk(self.__dict__['_basepath']):
            for file in f:
                if file.endswith(".db"):
                       collection_name = file.replace(".db","")
                       self[collection_name].get("")

        # Index/Subscription-functions

        """
        adding text index
        :name: index name
        :key: document field
        """
        def register_text_index(self,name,key,**kwargs):
            admin = {}

            
            path = Path(self._db.__dict__['_basepath']+os.sep+"admin.db")    
            path.parent.mkdir(parents=True, exist_ok=True) 
            
            lock = SoftFileLock(str(path.absolute())+".lock")
            
            try:
                    with  lock.acquire(timeout=self._db['_timeout']):
                        
                        if os.path.isfile(str(path.absolute())):
                            with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                    admin = json.load(f)
                                    f.close()

                        if 'text_indexes' in admin:
                                indexes = admin['text_indexes']
                        else:
                                indexes = {}    
                            
                        indexes[name]={"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}

                        admin['text_indexes']=indexes

                        self._db["db_indexes"][name] = {"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}
                        self._db["text_indexes"][name] = {"collection":self._name,"key":key,"dynamic":kwargs.get("dynamic")}

                        with open(str(path.absolute()),"w", encoding='utf-8') as f:    
    
                                    json.dump(admin,f)
                                    f.close()    


                                
                                
            except Timeout:
                    return -1

        """
        adding hash index
        :name: index name
        :key: document field
        """

        def register_hash_index(self,name,key,**kwargs):
            #self._db[self._name+"_"+name] = {}

            self._db._register_unique_index(self._name,name,key,**kwargs)


        """
        Register subscription

        :name: subscription name
        :collections: list of collections names

        """
        def create_subscription(self,name,collections): 
            self._register_subscription(name,collections) 

        """
        reindex hash-index

        :name: index name

        """
        def reindex_hash(self,name):
            indexes = self._db["hash_indexes"]
            index_settings = indexes.get(name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found') 
            
            key = index_settings['key']
            
            if index_settings.get("dynamic") == True:
                 self._db[name]._data = {}
                 for key_document,value_document in self._data.items():
                    if key in value_document:
                        id = hashlib.sha1(value_document.get(key).encode()).hexdigest()
                        self._db[name]._data[id] ={"_id":id,"value":key_document}
                 
            else:    
                self._db[name].clear(indexing=True)
                set = []
                for key_document,value_document in self._data.items():
                    if key in value_document:
                        set.append({"_id":hashlib.sha1(value_document.get(key).encode()).hexdigest(),"value":key_document})
                
                self._db[name].insert_many(set,NoIndex=True,upsert=True)

        """
        reindex function for text indexes

        :index_name: name of a index

        """       
        def reindex_text(self,index_name):
             
             #Надо поставить ограничение на маленькие размеры списков
             indexes = self._db["db_indexes"]
             index_settings = indexes.get(index_name)
             
             if  index_settings==None:
                  raise ValueError(f'No index settings found')  

             
             a = {}
             data = self._data.copy()
             #start_time = time.time()
             for id,document in data.items():
                  for key,value in document.items():
                       if key == index_settings['key']:
                            a[id] =value
             #print("preparing array: --- %s seconds ---" % (time.time() - start_time))               
             #start_time = time.time()
             slist =split_list2(a)
             #print("split: --- %s seconds ---" % (time.time() - start_time))               

             if index_settings.get("dynamic") == True:
                 collection = self._db[index_name]
                 collection._data= slist

             else: 
                 collection = self._db[index_name]
                 collection._data= slist

                 l = []
                 if "0" in slist:
                    l.append(slist["0"])
                 if "1" in slist:
                    l.append(slist["1"])
                 self._db[index_name].insert_many(l,upsert=True,NoIndex=True)    

        
        
        #*******************************Internal functions**************
        #

        
        """
        insert ONE value into the text indexes

        :index_name: name of a index
        :value: value too be inserting

        """  
        def _insert_value_text_index(self,index_name,value):
            indexes = self._db["text_indexes"]
            index_settings = indexes.get(index_name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            index_key = index_settings['key']

            collection = self._db[index_name]
            collection._recording = True
            index =  collection._data 
            if index_key in value:
                insert_in_branchces_dynamic_text_binary(index,value["_id"],value[index_key])


            if not index_settings.get("dynamic") == True:
                 #self._db[index_name]._recording = True
                 #self._db[index_name]._data = index
                 #l = []
                 #l.append(index["0"])
                 #l.append(index["1"])
                 #self._db[index_name].insert_many(l,upsert=True,NoIndex=True)
                  
                 lock = collection._lock_collection() 
                 l = []
                 if "0" in index:
                    l.append('"0":' + to_json_str(index["0"]) )
                 if "1" in index:
                    l.append('"1":' + to_json_str(index["1"]) )
                   
                 collection._write_collection("\n".join(l))  
                 collection._release_collection(lock)     
            collection._recording = False
                              
        """
        Writing the entire collection data to a file.
        Used in some situations
        :content: text string to be stored
        """
        def _write_collection(self,content):
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 

            prefix = str(uuid.uuid4())

            with open(self._path, 'a', encoding='utf-8') as f:
                f.seek(0)
                f.truncate()
                f.write(prefix)
                f.write("\n")
                f.write(content)
                f.write("\n")
        
        def _add_value_to_text_indexes(self,document):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      if value['key'] in document:
                           self._insert_value_text_index(index,document)

        def _add_values_to_text_indexes(self,documents):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      for document in documents:
                        if value['key'] in document:
                            self._insert_value_text_index(index,document)                   
                                 

        def _delete_value_text_index(self,index_name,value):
            indexes = self._db['text_indexes']
            index_settings = indexes.get(index_name)
             
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            index_key = index_settings['key']

            collection =  self._db[index_name]           
            collection._recording = True
            index =  collection._data 


            if index_key in value:
                delete_in_branchces_dynamic_text_binary(index,value["_id"],value[index_key]) 

            if not index_settings.get("dynamic") == True:
                 #l = []
                 #l.append(index["0"])
                 #l.append(index["1"])
                 #self._db[index_name].insert_many(l,upsert=True,NoIndex=True)
                 
                 lock = collection._lock_collection() 
                 l = []
                 l.append('"0":' + to_json_str(index["0"]) )
                 if "1" in index:
                    l.append('"1":' + to_json_str(index["1"]) )
                   
                 collection._write_collection("\n".join(l))  
                 collection._release_collection(lock)       

            collection._recording = False     
            

        def _delete_value_from_text_indexes(self,document):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      if value['key'] in document:
                           self._delete_value_text_index(index,document)

        def _delete_values_from_text_indexes(self,documents):
            indexes = self._db['text_indexes']
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      for document in documents:
                        if value['key'] in document:
                            self._delete_value_text_index(index,document)                             

        
        """
        Search substring in a text index
        :s: search string
        """
        def search_text_index(self,s):
             
            indexes = self._db["text_indexes"]
            index_settings = indexes.get(self._name)
            
            if  index_settings==None:
                  raise ValueError(f'No index settings found')  

            key = index_settings['key']

            result = []
            data = self._db[index_settings['collection']]
             #if index_name in self._db[index_name]:
            
            index =  self._data
            #start_time = time.time()
            ids = get_index_ids_by_string(index,s)
            #print("extract indexes: --- %s seconds ---" % (time.time() - start_time))  

            #start_time = time.time()
            for i in ids:
                elem  = data.get(i)
                if key in elem:
                    if s in elem[key]:  
                            result.append(elem)
            #print("lookup in indexes: --- %s seconds ---" % (time.time() - start_time))            
            return result         
                  
        """
        initialize data at first call
        """
        def __getattr__(self, item):
            if item=='_data':
                self._data = self._read_collection()

                return self._data
            
        def __getattribute__(self, item):
            if item=='_data': #return collection dictionary. Read data from file if necessary
                if self._is_index():
                     settings = self._db['db_indexes'].get(self._name)
                     if not settings.get("dynamic")==True:
                        
                        if self._is_modification(): #read data only if was a modifiation
                            self._data = self._read_collection()     
                        
                        """if object.__getattribute__(self, '_data')==None:
                            path = self._path.replace('json','npy')
                            if os.path.isfile(path):
                                nparr =  np.load(path,allow_pickle=True)
                                self._data=self._data =dict(nparr)
                            else:
                                self._data =  {}    """
                    
                            

                     
                else:     
                     if self._is_modification():
                        self._data = self._read_collection()
                
                return object.__getattribute__(self, '_data')

                
            else:
                return super().__getattribute__(item)   

        """
        generates document ID
        """
        def _get_next_id(self):
            
            next_id = str(uuid.uuid4())

            return next_id
        """
        check data modified by another process
        """
        def _is_modification(self):
            if  self._recording==True:
                 return False
            
            if  self._modification_uuid==None:
                 return True
            
            
              
            uid=None
            with open(self._path,"r", encoding='utf-8') as f:
                text =f.read(36)
                uid = text[:36]

            return uid!=self._modification_uuid
        
        
        

        
        
        """
        check collection is index
        """
        def _is_index(self):
            #indexes = self._db._get_unique_indexes()
                 
            return self._name in self._db["db_indexes"].keys()


        """
        Add value to hash-index
        :document: document to be stored to index
        :doc_id: ID of a document
        """
        def _add_value_to_unique_indexes(self,document,doc_id):
            #indexes = self._db._get_unique_indexes()
            indexes = self._db["hash_indexes"]
            
            
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      if value['key'] in document:
                           if not(isinstance(document.get(value['key']),dict) or isinstance(document.get(value['key']),list)):
                            new_index = {"_id":hashlib.sha1(document.get(value['key']).encode()).hexdigest(),"value":doc_id}
                            if value.get("dynamic") == True:
                                  self._db[index]._data[new_index["_id"]]=new_index
                            else:     
                                #self._db[index].insert(new_index,indexing=True)
                                self._db[index].fast_insert(new_index,upsert=True, NoIndex=True)
        def _delete_value_from_unique_indexes(self,document,doc_id):
            indexes = self._db._get_unique_indexes()
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      if value['key'] in document:
                           if isinstance(document.get(value['key']),str):
                            self._db[index].delete(hashlib.sha1(document.get(value['key']).encode()).hexdigest())
                                              

        def _add_values_to_unique_indexes(self,documents):
            indexes = self._db._get_unique_indexes()
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      new_indexes = []
                      for document in documents:
                        if value['key'] in document:
                            if not(isinstance(document.get(value['key']),dict) or isinstance(document.get(value['key']),list)):
                                new_index = {"_id":hashlib.sha1(document.get(value['key']).encode()).hexdigest(),"value":document['_id']}
                                new_indexes.append(new_index)
                                
                           
                      self._db[index].insert_many(new_indexes,upsert=True,NoIndex=True)
                     

        def _delete_values_from_unique_indexes(self,documents):
            indexes = self._db._get_unique_indexes()
            for index,value in indexes.items():
                 if value['collection'] == self._name:
                      todelete_indexes = []
                      for document in documents:
                        if value['key'] in document:
                            if isinstance(document.get(value['key']),str):
                                
                                todelete_indexes.append({"_id":hashlib.sha1(document.get(value['key']).encode()).hexdigest()})
                                
                           
                      self._db[index].delete_many(todelete_indexes,NoIndex=True)
                                    

                          
             


        """
        Read all data from collection file
        """
        def _read_collection(self):
            
            collection = {}
                
            if os.path.isfile(self._path):
                    path = Path(self._path)    
                    path.parent.mkdir(parents=True, exist_ok=True) 

                    lock = SoftFileLock(self._path+".lock")
                    try:
                        with  lock.acquire(timeout=self._db['_timeout']):
                            #start_time = time.time()
                            with open(self._path,"r", encoding='utf-8') as f:
                                content = f.readlines()
                                txt = ",".join(content[1:])
                                #txt = txt.replace("\n","")
                                collection = from_json_str("{"+txt+"}")    
                                uid = content[0].rstrip()
                                self._modification_uuid =uid

                            #print("decode: --- %s seconds ---" % (time.time() - start_time))     

                    except Timeout:
                        return -1

            return collection

        """
        Collection initialization
        :name: collection name
        :db_instance: database instance

        """
        def __init__(self,name: str,db_instance):
            
            self._db = db_instance
            self._name = name            
            self._path =db_instance['_basepath'] +os.sep+name+".db"
            
            
            
            self._next_id = None
       

        """
        Inserts or update document as text block into file 
         and value into collection dictionary

        :document: document or list of documents to be added
        
        Arguments:
        :NoIndex: OFF to change indexes
        :upsert: INSERT or UPDATE
        :update: only update existig
        :session: transaction instance


        """
        def fast_insert(self,document, **kwargs):
            
            collection = self._data

            self._recording = True

            def updater():
                    
                    no_search = False
                    if "_id" in document:
                        doc_id = document['_id']
                    else:
                        doc_id = self._get_next_id()     
                        document["_id"]  = doc_id
                        no_search=True
                    
                     
                    if doc_id in self._data:
                        no_search=False
                        if not (kwargs.get("upsert")==True or kwargs.get("update")==True):
                            
                            raise ValueError(f'Value with ID {str(doc_id)} already exists')
                    else:
                        no_search =True 

                    
                    
                    
                    self._data[doc_id] = dict(document)

                    doc_id = document['_id']

                    line =    '"'+ doc_id+'":'+ to_json_str(document) 
                    
                    return no_search,doc_id,line
                    

            if 'session' in kwargs: #updating with transaction
                    session = kwargs['session']
                    no_search,doc_id,line = self._update_collection_memory_fast(updater,session)
                    if no_search:
                        if not self._name in session._operations_add:
                             session._operations_add[self._name] = []
                        
                        session._operations_add[self._name].append(line)

                    else:    
                        if not self._name in session._operations_replace:
                             session._operations_replace[self._name] = []

                        session._operations_replace[self._name].append((doc_id,line))

                    if not kwargs.get("NoIndex") == True:  
                        session._related_add.append((self._name,document,doc_id))
            else:   #update directly 
                    doc_id = self._update_collection_fast(updater)

                    self._recording = False

                    if doc_id == -1:
                        raise ValueError('Write failed')
                    else:
                        if not kwargs.get("NoIndex") == True:   
                            self._add_value_to_unique_indexes(document,doc_id)
                            self._add_value_to_text_indexes(document)
                            self._db._add_value_to_subscriptions(self._name,doc_id)

            return doc_id        

        """
        Remove documents as text block from file 
        and value from collection dictionary

        :document: document or list of documents to be added
        
        Arguments:
        :NoIndex: OFF to change indexes
        :session: transaction instance


        """
        def fast_delete(self,document, **kwargs):
            
            collection = self._data

            self._recording = True

            def updater():
                if "_id" in document:
                    doc_id = document['_id']

                if not doc_id in self._data:
                    raise ValueError(f'Value with ID {str(doc_id)} not in collection')
                
                self._data.pop(doc_id, None)
                
                return False,doc_id,""
                    

            if 'session' in kwargs: #updating with transaction
                    session = kwargs['session']
                    no_search,doc_id,line = self._update_collection_memory_fast(updater,session)
                    if not self._name in session._operations_replace:
                        session._operations_replace[self._name] = []

                    session._operations_replace[self._name].append((doc_id,line))

                    if not kwargs.get("NoIndex") == True:  
                        session._related_add.append((self._name,document,doc_id))
            else:    #update directly 
                    doc_id = self._update_collection_fast(updater)

                    self._recording = False

                    if doc_id == -1:
                        raise ValueError('Write failed')
                    else:
                        if not kwargs.get("NoIndex") == True:   
                            self._add_value_to_unique_indexes(document,doc_id)
                            self._add_value_to_text_indexes(document)
                            self._db._add_value_to_subscriptions(self._name,doc_id)                    

        
        """
        Insert/Update/Upsert documents as text block from file 
        and value from collection dictionary

        :document: document or list of documents to be added
        
        Arguments:
        :NoIndex: OFF to change indexes
        :session: transaction instance
        :upsert: update existing, create if not exist
        :update: only update existig


        """
        def insert_many(self, dataset, **kwargs) -> str:
                
    
            collection = self._data.copy()

            documents = copy.deepcopy(dataset)

            self._recording = True

            def updater():
                
                no_search_list_file=[]
                #search_list_file=[]

                ids =  []
              
                search = []  
                
                #sum1=0
                #sum2=0
                #sum3=0
                
                for document in documents:
                    #start_time=time.time()
                    if "_id" in document:
                        doc_id = document['_id']

                    else:
                        doc_id = self._get_next_id()
                        document ["_id"]  = doc_id 
                    ids.append(doc_id)
                    #sum1+=time.time()-start_time
                    
                    #start_time=time.time()
                    if doc_id in self._data:
                       search.append(document)
                       
                       if not (kwargs.get("upsert")==True or kwargs.get("update")==True):
                            raise ValueError(f'Value with ID {str(doc_id)} already exists')
                    else:
                       if not kwargs.get("update")==True:
                          
                        no_search_list_file.append('"'+doc_id+'":'+ to_json_str(document) )
                    #sum2+=time.time()-start_time    
                    
                    #self._data[doc_id] = dict(document)
                    #start_time=time.time()
                    if kwargs.get("update")==True:
                         if doc_id in self._data:
                            self._data[doc_id] = dict(document)
                    else:        
                         self._data[doc_id] = dict(document)
                    #sum3+=time.time()-start_time
                #print("sum1: --- %s seconds ---" % sum1)
                #print("sum2: --- %s seconds ---" % sum2)
                #print("sum3: --- %s seconds ---" % sum3)

                return ids,search,no_search_list_file

           
            
            if 'session' in kwargs:
                session = kwargs['session']
                res,search,no_search_list_file = self._update_collection_memory(updater)
                if not self._name in session._operations_add:
                    session._operations_add[self._name] =[]     
                if len(no_search_list_file)>0:
                    session._operations_add[self._name].append(no_search_list_file)

                if not self._name in session._operations_replace:
                    session._operations_replace[self._name] =[]  

                if len(search)>0:
                    session._operations_replace[self._name].append(search)

                if not kwargs.get("NoIndex") ==True:
                        for document in documents:
                            session._related_add.append((self._name,document,document.get('_id')))
                        
                
            else:    
                res = self._update_collection(updater)
                if res == -1:
                    raise ValueError('Write failed')
                

                self._recording = False
                
                if not kwargs.get("NoIndex") ==True:
                    self._add_values_to_unique_indexes(documents)
                    if kwargs.get("update")==True:
                        self._delete_values_from_text_indexes(documents)     
                    self._add_values_to_text_indexes(documents)
                    self._db._add_values_to_subscriptions(self._name,documents)

                
                
            return res
        
        """
        Removes list of documents as text block from file 
        and value from collection dictionary

        :dataset: document or list of documents to be deleted
        
        Arguments:
        :NoIndex: OFF to change indexes
        :session: transaction instance


        """

        def delete_many(self, dataset, **kwargs) -> str:
                
            ids = []

            collection = self._data.copy()

            documents = copy.deepcopy(dataset)

            self._recording = True

            def updater():

                no_search_list_file=[]
 
                ids =  []

                search = []  
                for document in documents:
                    if "_id" in document:
                        doc_id = document['_id']

                    ids.append(doc_id)
                    
                    if not doc_id in self._data:
                        raise ValueError(f'Value with ID {str(doc_id)} not in collection')
                    
                    self._data.pop(doc_id)
                    search.append({"_id":doc_id,"$delete":True})
                    
                return ids,search,no_search_list_file
                
                #self._data = collection
 
            
            start_time = time.time()
            if 'session' in kwargs:
                session = kwargs['session']
                
                res,search,no_search_list_file = self._update_collection_memory(updater)
                if not self._name in session._operations_add:
                    session._operations_add[self._name] =[]     
               

                if not self._name in session._operations_replace:
                    session._operations_replace[self._name] =[]  

                if len(search)>0:
                    session._operations_replace[self._name].append(search)

                if not kwargs.get("NoIndex") ==True:
                        for document in documents:
                            session._related_delete.append((self._name,document,document.get('_id')))
                
            else:    
                res = self._update_collection(updater)
                if res == -1:
                    raise ValueError('Write failed')
                #print("updating data: --- %s seconds ---" % (time.time() - start_time))

                self._recording = False
                
                if not kwargs.get("NoIndex") ==True:
                    self._delete_values_from_unique_indexes(documents)
                    self._delete_values_from_text_indexes(documents)
                   
                
            return res

        """
        Run updater and commit changes 
        
        :updater: updater function

        """
        def _update_collection_fast(self,updater: Callable[[Dict[int, Mapping]], None]):
            
            collection = self._data
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 
        
            lock = SoftFileLock(self._path+".lock")
            try:
                with  lock.acquire(timeout=self._db['_timeout']):
                        
                        prefix = str(uuid.uuid4())

                        #start_time=time.time()
                        no_search,doc_id,line = updater()  
                        #print("updater: --- %s seconds ---" % (time.time() - start_time))  
                        no_update_uuid=False
                        if no_search:
                            #start_time=time.time()
                            with open(self._path,"a", encoding='utf-8') as f: 
                                if self._modification_uuid==None:
                                     f.write(prefix)
                                     f.write("\n")
                                     self._modification_uuid=prefix
                                     no_update_uuid=True
                                     
                                
                                f.write(line) 
                                f.write("\n")
                            #print("no search save: --- %s seconds ---" % (time.time() - start_time))  
                           
                        else:
                            if line=="":
                                replace_value_in_file(self._path,'"'+doc_id+'":',line)     
                            else:         
                                replace_value_in_file(self._path,'"'+doc_id+'":',line+"\n")     

                        
                        
                        if not no_update_uuid:
                            #replace_value_in_file(self._path,self._modification_uuid,prefix+"\n")
                            # 
                            #start_time=time.time()  
                            write_prefix(self._path,prefix)
                            #print("write prefix: --- %s seconds ---" % (time.time() - start_time)) 
                            self._modification_uuid = prefix  

                        
                        return  doc_id               
            except Timeout:
                    return -1 


        """
        Run updater and commit changes 
        
        :updater: updater function
        
        """
        def _update_collection(self,updater: Callable[[Dict[int, Mapping]], None]):
            
            

            collection = {}

            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 
        
            lock = SoftFileLock(self._path+".lock")
            try:
                with  lock.acquire(timeout=self._db['_timeout']):
                         
                        #start_time=time.time() 
                        ids,search_list,no_search_list_file = updater()
                        #print("updater many: --- %s seconds ---" % (time.time() - start_time)) 

                        no_update_uuid= False

                        prefix = str(uuid.uuid4())

                        if ids==["$clear"]:
                           with open(self._path,"a", encoding='utf-8') as f: 
                                f.seek(0)
                                f.truncate()
                                f.write(prefix)
                                f.write("\n")
                                self._modification_uuid=prefix
                                no_update_uuid=True

                                   
                        else:     
                            if len(no_search_list_file)>0:
                                #start_time=time.time() 
                                with open(self._path,"a", encoding='utf-8') as f: 
                                    
                                    if self._modification_uuid==None:
                                        f.write(prefix)
                                        f.write("\n")
                                        self._modification_uuid=prefix
                                        no_update_uuid=True

                                    f.write("\n".join(no_search_list_file)) 
                                    f.write("\n")
                                #print("write file addition many: --- %s seconds ---" % (time.time() - start_time))     
                            if len(search_list)>0:
                                #start_time=time.time()
                                replace_values_in_file(self._path,search_list)  
                                #print("write file replace values many: --- %s seconds ---" % (time.time() - start_time))        

                            if not no_update_uuid:
                            
                                #replace_value_in_file(self._path,self._modification_uuid,prefix+"\n")  
                                write_prefix(self._path,prefix)
                                self._modification_uuid = prefix         

            except Timeout:
                    return -1 
            
            return ids
        
        """
        Run updater only
        
        :updater: updater function
        
        """

        def _update_collection_memory(self,updater: Callable[[Dict[int, Mapping]], None]):
            collection = self._data

            res,search,no_search_list_file = updater()
            return res,search,no_search_list_file
        
        """
        Run updater only
        
        :updater: updater function
        
        """
        def _update_collection_memory_fast(self,updater: Callable[[Dict[int, Mapping]], None],session):
            
            collection = self._data

            no_search,doc_id,line = updater()
            return no_search,doc_id,line
        

        """
        Lock collection file
        
        """    
        def _lock_collection(self):
            
            path = Path(self._path)    
            path.parent.mkdir(parents=True, exist_ok=True) 
        
            lock = SoftFileLock(self._path+".lock")
            lock.acquire()

            return lock

        """
        Unlock collection file
        
        """  
        def _release_collection(self,lock):
            
            lock.release()    
            


    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        if key in self.__dict__:
            return self.__dict__[key]
        
        else:
            self.__dict__[key]=self.Collection(key,self)
            return self.__dict__[key]
        
                           
    
    def _register_subscription(self,name,collection_names):
        admin = {}

            
        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'subscriptions' in admin:
                            subscriptions = admin['subscriptions']
                    else:
                            subscriptions = {}    
                        
                    subscriptions[name]={"collections":collection_names}

                    admin['subscriptions']=subscriptions

                    with open(str(path.absolute()),"w", encoding='utf-8') as f:    
 
                                json.dump(admin,f)
                                f.close()    

                                
        except Timeout:
                    return -1    

    """
    Returns all the subscriptions
    """
    def _get_subscriptions(self):
        admin = {}
        subscriptions = {} 

        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'subscriptions' in admin:
                            subscriptions = admin['subscriptions']
                                 
        except Timeout:
                    return -1
        
        return subscriptions

    """
    Register value in subscription

    :collection: collection in which changes
    :doc_id:  documents ID

    """
    def _add_value_to_subscriptions(self,collection,doc_id):
            subscriptions = self._get_subscriptions()
            for subscription,value in subscriptions.items():
                 if collection in value['collections']:
                        new_record = {"_id":doc_id,"collection":collection}
                        self[subscription].fast_insert(new_record,upsert=True)

    def _add_values_to_subscriptions(self,collection,documents):
            subscriptions = self._get_subscriptions()
            for subscription,value in subscriptions.items():
                 if collection in value['collections']:
                      new_records = []
                      for document in documents:
                            new_record = {"_id":document["_id"],"collection":collection}
                            new_records.append(new_record)

                      self[subscription].insert_many(new_records,upsert=True,NoIndex=True)

                      
                           
                         
    
    
    
    def _get_unique_indexes(self):
        admin = {}
        indexes = {} 

        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'hash_indexes' in admin:
                            indexes = admin['hash_indexes']
                                 
        except Timeout:
                    return -1
        
        return indexes
    
    def _get_text_indexes(self):
        admin = {}
        indexes = {} 

        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'text_indexes' in admin:
                            indexes = admin['text_indexes']
                                 
        except Timeout:
                    return -1
        
        return indexes

    def _register_unique_index(self,collection_name,name,key,**kwargs):
        admin = {}

            
        path = Path(self.__dict__['_basepath']+os.sep+"admin.db")    
        path.parent.mkdir(parents=True, exist_ok=True) 
        
        lock = SoftFileLock(str(path.absolute())+".lock")
        
        try:
                with  lock.acquire(timeout=self.__dict__['_timeout']):
                      
                    if os.path.isfile(str(path.absolute())):
                        with open(str(path.absolute()),"r", encoding='utf-8') as f:
                                admin = json.load(f)
                                f.close()

                    if 'hash_indexes' in admin:
                            indexes = admin['hash_indexes']
                    else:
                            indexes = {}    
                        
                    indexes[name]={"collection":collection_name,"key":key}

                    admin['hash_indexes']=indexes

                    self["db_indexes"][name]={"collection":collection_name,"key":key,"dynamic":kwargs.get("dynamic")}
                    self["hash_indexes"][name]={"collection":collection_name,"key":key, "dynamic":kwargs.get("dynamic")}

                    with open(str(path.absolute()),"w", encoding='utf-8') as f:    
 
                                json.dump(admin,f)
                                f.close()    
                                
                                
        except Timeout:
                    return -1
        


    def __init__(
        self,
        name: str,
        **kwargs
      
    ):
       
        if 'path' in kwargs:
            basepath = kwargs.get("path")+os.sep+name
        else:
            basepath =os.path.dirname(os.path.realpath(__file__))+os.sep+name
        
        self._name = name
        

        #self._collections: Dict[str, Collection] = {}
        self.__dict__: Dict[str, self.Collection] = {}
        self.__dict__['_basepath']=basepath
        
        if 'timeout' in kwargs:
            self.__dict__['_timeout']=kwargs.get("timeout")
        else:    
            self.__dict__['_timeout']=LOCK_TIMEOUT

        hash_indexes = self._get_unique_indexes()
        text_indexes = self._get_text_indexes()
        common = dict(hash_indexes)
        common.update(text_indexes)
        self.__dict__['db_indexes']  = common
        self.__dict__['text_indexes']  = text_indexes
        self.__dict__['hash_indexes']  = hash_indexes

    def collection(self, name: str, **kwargs) -> Collection:
        

        if name in self.__dict__:
            return self.__dict__[name]

        collection = self.Collection(name, self)
        self.__dict__[name] = collection

        return collection
    

"""
a class that organizes a transaction within itself
"""
class DBSession:
    def __init__(self, db: SimpleBase) -> None:
        self._operations = {}
        
        self._operations_add = {}
        self._operations_replace = {}

        self._related_add = []
        self._related_delete = []
        self._db = db
    """
    commit transaction: store collections to files
    """
    def commit(self):

        list_locks = {}
        for collection_name, value in self._operations_add.items():
            lock_required = False
            if isinstance(value,list):
                 if len(value) >0:
                      lock_required=True
            else:
                 lock_required=True
            if lock_required:               
                lock = self._db[collection_name]._lock_collection()
                list_locks[collection_name]=lock
        
        for collection_name, value in self._operations_replace.items():
            if not collection_name in list_locks:
                lock_required = False
                if isinstance(value,list):
                    if len(value) >0:
                        lock_required=True
                else:
                    lock_required=True
                if lock_required:
                    lock = self._db[collection_name]._lock_collection()
                    list_locks[collection_name]=lock    

        prefix = str(uuid.uuid4())
        no_update_uuid=False

        for collection_name, value in self._operations_add.items():
            collection = self._db[collection_name]
            if isinstance(value,list):
                 
                 if len(value)>0:
                    with open(collection._path,"a", encoding='utf-8') as f: 
                        if collection._modification_uuid==None:
                            
                            f.write(prefix)
                            f.write("\n")
                            collection._modification_uuid=prefix
                            no_update_uuid=True
                        for line in value:            
                            if isinstance(line,str):
                                f.write(line) 
                            else:
                                txt  = "\n".join(line) 
                                f.write(txt)   
                            f.write("\n")


 
            if not no_update_uuid:
                #replace_value_in_file(collection._path,collection._modification_uuid,prefix+"\n")  
                write_prefix(collection._path,prefix)
                collection._modification_uuid = prefix                 

        for collection_name, value in self._operations_replace.items():
            collection = self._db[collection_name]

            if isinstance(value,list):
                 
                 if len(value)>0:
                    for set in value:
                        if isinstance(set,list):
                            
                                replace_values_in_file(collection._path,set) 
                        else:     
                            doc_id = set[0]  
                            line = set[1]  
                            if line=="":
                                    replace_value_in_file(collection._path,'"'+doc_id+'":',line)     
                            else:     
                                    replace_value_in_file(collection._path,'"'+doc_id+'":',line+"\n")     

            if not no_update_uuid and collection._modification_uuid!=prefix:
                #replace_value_in_file(collection._path,collection._modification_uuid,prefix+"\n")     
                write_prefix(collection._path,prefix)
                collection._modification_uuid = prefix
                        
                        
         
                          

        for collection_name, value in self._operations_add.items():
            lock = list_locks[collection_name]
            
            self._db[collection_name]._recording=False

            self._db[collection_name]._release_collection(lock)

        for collection_name, value in self._operations_replace.items():
            lock = list_locks[collection_name]
            
            self._db[collection_name]._recording=False

            self._db[collection_name]._release_collection(lock)      
            

        #db[collection_name]._add_values_to_unique_indexes()
        for collection_name in  self._operations.keys():
            #updating indexes for insert,update operations    
            set = []
            for c_name,document,doc_id in self._related_add:
                 if c_name == collection_name:
                    set.append(document)
            #db[collection_name]._add_value_to_unique_indexes(document,doc_id)
            #db._add_value_to_subscriptions(collection_name,doc_id)     
            self._db[collection_name]._add_values_to_unique_indexes(set)
            self._db[collection_name]._add_values_to_text_indexes(set)
            self._db._add_values_to_subscriptions(collection_name,set)

            set = []
            for c_name,document,doc_id in self._related_delete:
                 if c_name == collection_name:
                    set.append(document)
            #db[collection_name]._add_value_to_unique_indexes(document,doc_id)
            #db._add_value_to_subscriptions(collection_name,doc_id)     
            self._db[collection_name]._delete_values_from_unique_indexes(set)
            self._db[collection_name]._delete_values_from_text_indexes(set)
 

        self._operations = {}
        
        self._operations_add = {}
        self._operations_replace = {}

        self._related_add=[]
        self._related_delete=[]

    def __enter__(self) -> "DBSession":
        
        return self        
    """
    left the "with" operator
    """    
    def __exit__(self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType]) -> bool:

        if value == None and not traceback:
            self.commit()
            return True
        else:
            for collection_name in self._operations.keys():
                self._db[collection_name]._modification_uuid=None
                c = self._db[collection_name]
            return False

          


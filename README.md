# SimpleBase
Lightweight JSON-oriented database

SimpleBase is a JSON based serverless database with improved performance on key operations
• Instant addition of new records to collections, regardless of the size of the collection due to a special storage architecture
• Faster work with operations, due to the fact that it is not required to encode / decode the entire collection (which can be very large)
• Collections are always stored in RAM with concurrency change tracking: data is re-read from disk only if it has been changed by another process
• ACID for multi-user and multi-threading
• Two types of indexes for key types of queries - a hash index and a special b-tree for full-text search
• Support for transactions (sessions)
• MongoDB-like interface, incl. 100% similar query language
• Written in pure python, only about 1800 lines

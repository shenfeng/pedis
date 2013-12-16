listdb
======

A fast and lightweight key/list database server.

**working in progress**

#### Design goal

Use redis's protocol, support `set`, `get`, `lrange`, `lpush`, `rpush` command. Data is stored on the disk instead of RAM.

I want to use it to store user's activities on my website, and then do personal recomendation according to his history actions.

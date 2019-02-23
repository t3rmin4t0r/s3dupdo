# s3dupdo
S3 data duplicator

# Using it has two steps 

Plan (collects a src path + dst path) & writes an SQLite file with all the files to be copied

Run goes over the sqlitefile (in as many threads as you wish) and copies the files one by one, committing them to SQLite

So you can even begin a run on one machine, ^C and start over on another machine.

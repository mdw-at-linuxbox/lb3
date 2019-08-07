=========
Load test
=========

This is intended to provide a discrete
unit of workload for a ceph radosgw.

It reads in a file which tells it what to do,
then spawns one or more threads and does that.

This version creates objects using s3, but is otherwise
generally similar to
`doad <https://github.com/mdw-at-linuxbox/doad>`_.
Note that rndrw.pl available from that repository
can be used to make input files for this tool.

Both of these are quick and dirty hacks.
Packaging and documentation is limited.

Building
--------
Edit the makefile to point to where you have previously
installed aws-sdk-cpp.
then `make`.
Output executable is `lb3`;
use it in place or install it where you will.

Input file
----------

lb3 reads in a file which consists of 0 or more lines of the form:

- `ADD` objectname
  create an object.  If `-s` is not specified, contents come from that name file.
- `DEL` objectname
  remove an object, which should have been created via an earlier ADD.
- `MKB` bucketname
  make a new bucket.
- `RMB` bucketname
  remove a bucket, which may have been created via an earlier MKB.

Options
-------

- `-t` count, make up to this many requests in parallel.
- `-e` endpoint, required
- `-b` bucket, usually required
- `-s` size, specify a fixed size (decimal,0xhex,0octal).
  if this is specified, any objects made will be set
  to this number of 0's.  Otherwise, a file matching the
  object name will be read.
- `-w` when removing a bucket, don't remove it until it is made.
- `-v` emit more information about what is going on.

Credentials can be passed one of these 3 ways,

- set ~/.aws/credentials
- point env AWS_SHARED_CREDENTIALS_FILE to credentials elsewhere
- set AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

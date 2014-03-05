HandlerSocket4Java
==================

A handlersocket client for java, base on netty 4.0.x.
I hope it may be very fast, but it doesn't meet my expectations.
DB server and client in two different '8-core 1.6G' server, 24G memory, create/delete/modify only about 14,000 tps, and query only about 12,000 tps.
Use innodb, and innodb_buffer_size is 8G. There are 6 segments in the table.

In any case, it is a example for netty 4.0.x. I learned much from the working. The code is very clean, and very easy to read.
I used some codes from another projects in github.


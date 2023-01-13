Shareablee Recruitment Task
===========================

Goals of this task:

1. Make sure all pipeline runs smoothly (`make test`)
2. Make sure application (`main.py`) runs correctly
    Application should read all documents from index1, transform document and write into index2.
    The transformation should generate document with all fields from original
    document and also with additional field called `calculated` which contains
    number of characters in original document (from both keys and values).
3. Make sure Application can process 1mln documents.
4. optional (if you have time): make sure application is optimized for time and memory usage.
5. optional (if you have time): prepare plot of computational complexity of the application (you can produce that graph with libreoffice or what)

ElasticSearch Transformer
=========================

ElasticSearch Transformer read ALL documents from one index, transform the values and write to second ElasticSearch index.

Application suite contains test case to validate that application is working correctly by generating test data set, running application and verifying results of application output.

How to run it test case
-----------------------

1. Must have docker installed
2. run `make test` or run docker manually with command from `Makefile`

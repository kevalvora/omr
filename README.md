# OMR: Out-of-Core MapReduce for Large Data Sets
OMR is a single machine MapReduce system to efficiently processes very large datasets that do not fit in main memory. It actively minimizes the amount of data to be read/written to/from disk via type-aware meta-data optimizations and on-the-fly aggregation. With minimized random disk I/O, OMR provides linear scaling with growing datasets, and significantly outperforms state of the art MapReduce systems like Hadoop and Metis. System details and performance results can be found in the [ISMM paper](https://doi.org/10.1145/3210563.3210568).

## Compiling Applications
Following compilers/libraries are required: 
* C++11 Compiler.
* [Protocol Buffers](https://developers.google.com/protocol-buffers/)

```
apt-get install libprotobuf-dev protobuf-compiler libgoogle-perftools-dev
```

To compile, go to the specific application directory and run:

```
make
```

For example, to compile word count application, run:

```
cd examples/wordcount
make
```

Note that above will produce two binaries: 
* `wordcount.bin` (which uses variable sized records)
* `wordcount-one.bin` (which uses fix-sized records)

## Running Applications
You may need to raise the limit of maximum open file descriptors. You can do so by running:

```
ulimit -n 1048576
```

To run the application, you need to give the directory containing input files, number of mapper and reducer threads, batch size (which indicates number of key-value pairs that can be held by a thread in map phase), and kitems (which indicates number of key-value pairs that should be read by a thread from intermediate files during reduce phase):

```
<application_name> <folderpath> <nmappers> <nreducers> <batchsize> <kitems>
```

For example, to run word count with fixed-sized records:

```
./wordcount-one.bin ../../inputs/ 8 8 10000000 10000000
```

You can use other applications by replacing the binary name as follows:

```
./wordcount-one.bin ../../inputs/ 8 8 10000000 10000000
./wordcount.bin ../../inputs/ 8 8 10000000 10000000
./invertedindex-one.bin ../../inputs/ 8 8 10000000 10000000
./invertedindex.bin ../../inputs/ 8 8 10000000 10000000
./degreecount-one.bin ../../inputs/ 8 8 10000000 10000000
./degreecount.bin ../../inputs/ 8 8 10000000 10000000
./adjacencylist.bin ../../inputs/ 8 8 10000000 10000000
```

## Resources
Gurneet Kaur, Keval Vora, Sai Charan Koduru, and Rajiv Gupta. OMR: Out-of-Core MapReduce for Large Data Sets. International Symposium on Memory Management, 12 pages, Philadelphia, Pennsylvania, June 2018.

To cite OMR, you can use the following BibTeX entry:

```
@inproceedings{Kaur:2018:OOM:3210563.3210568,
 author = {Kaur, Gurneet and Vora, Keval and Koduru, Sai Charan and Gupta, Rajiv},
 title = {OMR: Out-of-core MapReduce for Large Data Sets},
 booktitle = {Proceedings of the 2018 ACM SIGPLAN International Symposium on Memory Management},
 series = {ISMM 2018},
 year = {2018},
 isbn = {978-1-4503-5801-9},
 location = {Philadelphia, PA, USA},
 pages = {71--83},
 numpages = {13},
 url = {http://doi.acm.org/10.1145/3210563.3210568},
 doi = {10.1145/3210563.3210568},
 acmid = {3210568},
 publisher = {ACM},
 address = {New York, NY, USA},
} 
```

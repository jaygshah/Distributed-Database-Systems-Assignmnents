CSE 512 - Assignment 4 - Equijoin using MapReduce
Name: Jay Shah
ASU ID: 1215102837
Approach


1. Driver: The driver function receives as inputs the input directory and output directory, it starts the job named equijoin and the required mapper and reducer classes are called. The driver waits for the code to run and exits once done.

2. Mapper: It processes the input file received as an input and creates a key-value pair for each tuple where the key is the integer join column value after table name and that whole tuple is used as value.

3. Reducer: We split the key-value pairs based on their table names, here 2, and then using the join-key we merge only those tuples present in both the separate tables and not otherwise. These tuples consist the results of equijoin using MapReduce
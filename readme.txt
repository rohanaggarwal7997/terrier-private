This readme file summarizes the submission by
Preetansh Goyal
Gautam Jain
Rohan Aggarwal
for project 2 of 15721 - Advanced Database Systems.


We implemented BPlusTree with optimistic latch grabbing protocol for inserts and deletes. Our ScanKey and ScanAscending take read locks and move in forward direction. Our ScanDescending moves in the reverse direction and needs to start again if it cannot acquire a readlock. This happens as we take write lock of right sibling during inserts and deletes which might be a child of another parent.

We have written a large amount of tests to test our implementation which are present in bplustreetest.cpp. Following is a short
summary of them - 

1) Node Tests - These tests verify all elementary node functions like - SplitNode, PopBegin, PopEnd, Merge etc.
2) Insert and Delete tests - These tests try to test inserts and deletes in a variety of different situations and focus on getting everything right. Things like no splitting, splitting, merging, deletes after inserts and other border cases are extensively checked. There are checks for multiple values too and inserting same values again too for the same key.
3) StructuralIntegrityTests - These tests perform a lot of different operations on the bplustree and check at each time whether the structural integrity of the bplustree is maintained.
We have a structural integrity test similar to what the write up mentioned that goes inside the tree, checks all nodes, their sizes, their values are present in a range and recurses on the children. We have verified through extensive testing that none of our operations violate the structural integrity of the tree.
4) LargeKeyRandomInsertSiblingSequenceTest, KeyRandomInsertAndDeleteSiblingSequenceTest
	Sibling Pointers checks - These tests verify that all the sibling pointers are maintained correctly and we can do scans.


We have further evaluated the correctness of our implementation by doing the following checks. All numbers
are on Amazon c5.9xlarge instance

1) Ran tpcc_test, bplustree_index_test, bplustree_test in debug mode
2) Ran bplustree_index_test and bplustree_test in release mode
Release Mode - 
3) Ran a script that runs tpcc_test 100 times with 4 threads to detect race conditions. Benchmark output file present.
4) Ran a script that runs tpcc_test 100 times with 36 threads to detect race conditions. Benchmark output file present.

To evaluate the performance of our implementation we did the following

1) Comparision vs Bwtree on index_wrapper_benchmark in release mode

The bwtree over 5 iterations had the following run times on index_wrapper_benchmark (636.033k items/s, 642.094k items/s, 637.861k items/s, 637.623k items/s, 640.516k items/s) , the average of which is ()
The bplustree over 5 iterations had the following run times on index_wrapper_benchmark (624.627k items/s, 642.094k items/s, 620.6k items/s, 622.197k items/s, 626.172k items/s) , the average of which is ()

Hence clearly bplustree outerperforms bwtree.

Since index_wrapper_benchmark is only single threaded, we wanted to see how bplustree scales compared to bwtree. 

We compared performance of tpcc_test in release mode with 36 threads.
The bwtree over 5 iterations had the following run times on tpcc_test (51040 ms, 50967 ms, 50698 ms, 50293 ms, 51032 ms) , the average of which is 50806 ms.
The bplustree over 5 iterations had the following run times on tpcc_test (43692 ms, 43666 ms, 43956 ms, 43957 ms, 43968 ms) , the average of which is 43848 ms.

Clearly BplusTree has better scaling too. And hence serves as a successful replacement of Bwtree.
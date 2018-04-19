 
# EDGAR Analytics Submission
## Repo directory structure

The directory structure of the submission repo is as following:

    ├── README.md 
    ├── run.sh
    ├── src
    │   └── sessionization.py
    ├── input
    │   └── inactivity_period.txt
    │   └── log.csv
    ├── output
    |   └── sessionization.txt
    ├── insight_testsuite
        └── run_tests.sh
        └── tests
            └── test_1
            |   ├── input
            |   │   └── inactivity_period.txt
            |   │   └── log.csv
            |   |__ output
            |   │   └── sessionization.txt
            ├── kcg_test_01 
                ├── input
                │   └── your-own-inputs
                |── output
                    └── sessionization.txt
                    :
                    :
                    
            ├── kcg_test_11 
                ├── input
                │   └── your-own-inputs
                |── output
                    └── sessionization.txt

I strictly follow the required repo directory structure. In addition, I add 12 additional test (e.g. in the directory 'kcg\_test\_11') of various complexity as sanity check.

## Testing the directory structure and output format

Following the instructions, I run the test with the following command from within the `insight_testsuite` folder:

    insight_testsuite~$ ./run_tests.sh 


On success:

    [PASS]: kcg_test_01 sessionization.txt
    [PASS]: kcg_test_02 sessionization.txt
    [PASS]: kcg_test_03 sessionization.txt
    [PASS]: kcg_test_04a sessionization.txt
    [PASS]: kcg_test_04b sessionization.txt
    [PASS]: kcg_test_05 sessionization.txt
    [PASS]: kcg_test_06 sessionization.txt
    [PASS]: kcg_test_07 sessionization.txt
    [PASS]: kcg_test_08 sessionization.txt
    [PASS]: kcg_test_09 sessionization.txt
    [PASS]: kcg_test_10 sessionization.txt
    [PASS]: kcg_test_11 sessionization.txt
    [PASS]: test_1 sessionization.txt
    [Fri Apr  6 08:44:50 EDT 2018] 13 of 13 tests passed
		
Note that kcg\_test\_10  and kcg\_test\_11 takes respectively about eight and 30 seconds to run.


## Implementation notes

For my solution to scale,  I use the following data structures, concepts, and techniques:

### Python generator:

The log.csv file can be very large. For example, log20170630.csv has a size about 2.73 Gb and has about 23,677,600 records. As such, it is not practical and necessary to store all the record in the memory all at once. The generator is implemented in function ```get_log_data(csv_fname)``` 

### Hash table to track active IP addresses and associated information

I use a hash table (Python dictionary) ```dict_ip_sess```to keep track active IP addresses and the associated information. The hash table use IP addresses as keys. The value associated with each key is a tuple that contains the information such as session staring time, ending time, expiration time, and document name, etc. To maintain a small footprint of the table, the table is constantly updated, i.e., the key-value pairs of expired sessions are deleted.


### Priority queue to track inactive (expired) sessions

I use Python dictionary implementation of a priority queue (adapted from: https://gist.github.com/matteodellamico/4451520). The keys of the dictionary are IP addresses, and values are a tuple of timestamps to be used as priorities. In particular, the class ```priority_dict(dict) ``` is used for updating and removing inactive (expired) sessions. The advantage over a standard heapq-based priority queue is that priorities of items can be efficiently updated (amortized O(1)). To maintain a small footprint of the priority queue, at each new time slot, the session with the top priority (expiration time) is checked. If the expiration time is less than or equal to the current time stamp, this session is deemed inactive and thus removed from the priority queue. 

### Stress tests
I also run the following tests on the log files with very large number of records. The preliminary results show that running time increases linearly with the number of records.

| File name | No. of records| Max. no. of keys| Time (s) |
| :--------:|:-------------:|     :-----:   | :-----:|
| log20070401.csv  | 716,643 | 20 | 28
| log20141001.csv  | 9,125,753| 163 |343
| log20170630.csv  | 23,677,599 |282 |863



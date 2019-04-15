# Advanced Data Mining Project
Bradley Dice ([@bdice](https://github.com/bdice/)),
Shannon Moran ([@shannon-moran](https://github.com/shannon-moran/)),
Dan McCusker ([@dm1181](https://github.com/dm1181/))

## Overview

This repository contains code for using higher-order networks [1] to analyze U.S. airline ticket data from 1993 Q1 to 2018 Q3.
The data is provided by the Bureau of Transportation Statistics [2].
This project was completed for the course Advanced Data Mining with Danai Koutra (EECS 598, Winter 2019) at the University of Michigan [3].

## Airline Data Acquisition

This section refers to the code in the subdirectory `airline_data`, which fetches ticket information from the [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/Tables.asp?DB_ID=125).

### Installation

Use the requirements file (`pip install -U -r requirements.txt`) to get the dependencies for these data acquisition scripts.

Installation notes:

- It was necessary to use Python 2 for compatibility with Spark 2.2 on the flux-hadoop cluster.
- The dependency `cartopy` is much easier to install via `conda` than `pip`. It is only needed for generating map visualizations.

### Procedure

Run `python init.py` to create the data space.

Run `python project.py run --progress --show-traceback` to execute the workflow:

1. `fetch_data`: Fetches data with `urllib.request`.
2. `remove_readmes`: Removes `readme.html` from all zip files (having an extra file in the zip breaks `pandas`).
3. `determine_size`: Reads dataset sizes (rows, columns) from each zip file with `pandas` and saves that metadata to the job document.
4. `unzip_data`: Unzips the CSV file.
5. `label_data`: Renames the CSV file (for consistency across all jobs).
6. `get_IATA`: Locate unique airport codes in this quarter of data, for cross-reference with the IATA database.
7. `extract_edges`: Use PySpark to count the number of occurrences for each directed edge `(origin, destination)`, output to `edges.tsv/part-*`.
8. `combine_edgefile`: Combine the (distributed) outputs from `edges.tsv/part-*` into one file, `all_edges.tsv`.
9. `extract_itineraries`: Use PySpark to combine all trajectories from the same itinerary, output to `hon_itineraries.txt/part-*`.
10. `combine_itineraries`: Combine the (distributed) outputs from `hon_itineraries.txt/part-*` into one file, `all_hon_itineraries.txt`.

Users may specify which operations should be run with the `-o operation_name` flag, like `python project.py run -o fetch_data --progress --show-traceback`.
For the data generation in this project, operations 1-5 were run on a local directory.
Then, the data was copied into HDFS and operations 6-10 were run on HDFS (accessed via hadoop-fuse).

The command `python project.py status -d` shows the status of the workflow, including which operations will run next.

### Data Format

The `all_edges.tsv` file follows this format:

```
origin_id destination_id count
```

where `origin_id` and `destination_id` come from the DB1B data's identifiers and `count` is the number of coupons found for that edge.

The `all_hon_itineraries.txt` file follows the line input format required by the pyHON code [1]:

```
unique_id node1 node2 node3 node4 ... nodeN
```

where `unique_id` is an identifier for the whole trajectory and `node1` through `nodeN` are stops along that trajectory.

## References

1. Jian Xu, Thanuka L. Wickramarathne, Nitesh V. Chawla. [Representing higher-order dependencies in networks](http://advances.sciencemag.org/content/advances/2/5/e1600028.full.pdf). Science Advances 2, no. 5 (2016): e1600028. [GitHub repository](https://github.com/xyjprc/hon)
2. [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/DatabaseInfo.asp?DB_ID=125)
3. [EECS 598 W19 course webpage](http://web.eecs.umich.edu/~dkoutra/courses/W19_598/)

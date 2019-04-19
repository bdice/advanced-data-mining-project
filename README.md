# Community Detection on Higher-Order Networks: Identifying Patterns in US Air Traffic
Bradley Dice ([@bdice](https://github.com/bdice/)),
Shannon Moran ([@shannon-moran](https://github.com/shannon-moran/)),
Dan McCusker ([@dm1181](https://github.com/dm1181/)),
University of Michigan.

## Project Overview

In this project, we studied air traffic data from the last 25 years using _higher-order network representations_ [1].
Many airline passengers travel in round-trips and have layovers in hub airports for their carrier of choice.
Therefore, the place a passenger flies next strongly depends on the previous airports they've visited.
Network analyses that assume first-order Markov processes (random walks) on the network of US airports _cannot capture important dynamics and patterns_ in flight data, and higher-order networks are one solution to this problem.

Our analysis processes nearly one *billion* coupons obtained from the Bureau of Transportation Statistics [2], building higher-order networks on the itineraries and detecting communities with InfoMap [3] in the resulting graph structure.
All data and workflows were managed with the [_signac_ framework](https://signac.io) [4].
This project was completed for the course Advanced Data Mining with Danai Koutra (EECS 598, Winter 2019) at the University of Michigan [5].

## Airline Data Acquisition

The primary source code of this project is in the subdirectory `airline_data`. This contains code which fetches ticket information from the [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/Tables.asp?DB_ID=125) and analyzes it using PySpark, HON, and InfoMap [1, 2, 3].

### Installation

Use the requirements file (`pip install -U -r requirements.txt`) to get the dependencies for these data acquisition scripts.

Installation notes:

- It was necessary to use Python 2 for compatibility with Spark 2.2 on the flux-hadoop cluster.
- The dependency `cartopy` is much easier to install via `conda` than `pip`. It is only needed for generating map visualizations.

### Procedure

Run `python init.py` to create the _signac_ data space.

Run `python project.py run --progress --show-traceback` to execute the workflow. The operations below are performed independently for each signac "job," corresponding to one quarter of data. All file paths are relative to the job workspace directory.

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
11. `plot_first_order_pagerank`: Computes, saves, and plots PageRank data from the first-order network. Creates `first_order_pagerank.txt` and `pr_1st_order.png`.
12. `generate_hon`: Uses pyHON (modified from [original source code](https://github.com/xyjprc/hon) for ease of use) to generate the higher-order network. Produces `hon_rules.txt` and `hon_network.txt`.
13. `hon_pagerank`: Runs pyHON PageRank algorithm on higher-order network and saves PageRank data to `hon_pagerank.txt`.
14. `plot_hon_pagerank`: Plots PageRanks from nodes in the higher-order network, saved to `pr_hon.png`.
15. `hon_prepare_infomap`: Translates the format of the higher-order network to the [states format](http://www.mapequation.org/code.html#State-format) for processing with InfoMap. Produces `hon_infomap.txt`.
16. `hon_infomap`: Executes InfoMap algorithm, producing hierarchical cluster outputs in `hon_infomap.clu` and `hon_infomap.tree`.
17. `plot_hon_communities`: Plots each community with more than 0.5% of the network's flow, saved in `hon_community_plots/hon_community_*.png`.
18. `first_order_prepare_infomap`: Translates the format of the first-order network to the [Pajek format](http://www.mapequation.org/code.html#Pajek-format) for processing with InfoMap. Produces `first_order_infomap.txt`.
19. `plot_first_order_communities`: Plots each community with more than 0.5% of the network's flow, saved in `first_order_community_plots/first_order_community_*.png.
20. `plot_hon_top_communities`: Creates a plot similar to the previous HON community plots, overlaying the top 5 communities and coloring them by their respective clusters in `hon_top_communities.png`.
21. `plot_hon_community_carriers`: For each community with more than 0.5% of the network's flow, plot a pie chart of different airlines' presence within that community, weighted by the sum of origin and destination carrier flows. Produces `hon_community_plots/hon_community_carriers_*.png` and `hon_community_carriers.json`.

Users may specify which operation should be run with the `-o operation_name` flag, like `python project.py run -o fetch_data --progress --show-traceback`.
For the data generation in this project, operations 1-5 were run on a local directory.
Then, the data was copied into HDFS and operations 6-21 were run on HDFS (accessed via hadoop-fuse).

The command `python project.py status -d` shows the status of the workflow, including which operations will run next.

### Data Formats

The `all_edges.tsv` file follows this tab-separated format:

```
origin_id destination_id count
```

where `origin_id` and `destination_id` come from the DB1B data's identifiers and `count` is the number of coupons found for that edge.

The `all_hon_itineraries.txt` file follows the space-separated line input format required by the pyHON code [1]:

```
unique_id node1 node2 node3 node4 ... nodeN
```

where `unique_id` is an identifier for the whole trajectory and `node1` through `nodeN` are stops along that trajectory.

InfoMap uses the [states format](http://www.mapequation.org/code.html#State-format) and [Pajek format](http://www.mapequation.org/code.html#Pajek-format) described above.

## References

1. Xu, J., Wickramarathne, T. L., & Chawla, N. V. (2016). Representing higher-order dependencies in networks. _Science Advances_, _2_(5), e1600028. [Publication](https://doi.org/10.1126/sciadv.1600028), [GitHub repository](https://github.com/xyjprc/hon).
2. [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/DatabaseInfo.asp?DB_ID=125).
3. Rosvall, M., Esquivel, A. V, Lancichinetti, A., West, J. D., & Lambiotte, R. (2014). Memory in network flows and its effects on spreading dynamics and community detection. _Nature Communications_, _5_, 4630. [Publication](https://doi.org/10.1038/ncomms5630), [Website](http://www.mapequation.org/), [GitHub repository](https://github.com/mapequation/infomap).
4. Adorf, C. S., Dodd, P. M., Ramasubramani, V., & Glotzer, S. C. (2018). Simple data and workflow management with the signac framework. _Comput. Mater. Sci._, _146_(C):220-229, 2018. [Publication](https://doi.org/10.1016/j.commatsci.2018.01.035), [Website](https://signac.io), [GitHub repository](https://github.com/glotzerlab/signac).
5. [EECS 598 W19 course webpage](http://web.eecs.umich.edu/~dkoutra/courses/W19_598/).

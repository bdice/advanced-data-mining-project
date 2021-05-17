# Community Detection on Higher-Order Networks: Identifying Patterns in US Air Traffic

![GitHub](https://img.shields.io/github/license/bdice/advanced-data-mining-project.svg)
![GitHub top language](https://img.shields.io/github/languages/top/bdice/advanced-data-mining-project.svg)
[![Visualize with nbviewer](https://img.shields.io/badge/visualize-nbviewer-blue.svg)](https://nbviewer.jupyter.org/github/bdice/advanced-data-mining-project/tree/master/airline_data/exploration/)
[![Data managed with signac](https://docs.signac.io/en/latest/_images/signac.io-dark.svg)](https://signac.io)

Bradley Dice ([@bdice](https://github.com/bdice/)),
Shannon Moran ([@shannon-moran](https://github.com/shannon-moran/)),
Dan McCusker ([@dm1181](https://github.com/dm1181/)),
University of Michigan.

## Project Overview

In this project, we studied air traffic patterns from 1993 to 2018 using _higher-order network representations_ [1].
Many airline passengers travel in round-trips and have layovers in airports that act as hubs for their carrier of choice.
Because of patterns like these, the place a passenger flies next strongly depends on the previous airports they've visited.
Network analyses that assume first-order Markov processes (random walks) on the network of US airports _cannot capture important dynamics and patterns_ in flight data, and higher-order networks are one solution to this problem.

Our analysis processes **nearly one billion ticket coupons** covering 25 years of data obtained from the Bureau of Transportation Statistics [2].
We build higher-order networks on the itineraries and detect communities with InfoMap [3] in the resulting graph structure.
All data and workflows were managed with the [_signac_ framework](https://signac.io) [4].
This project was completed for the course Advanced Data Mining with [Danai Koutra](http://web.eecs.umich.edu/~dkoutra/) (EECS 598, Winter 2019) at the University of Michigan [5]. Read [the poster](Poster.pdf) or [the report](reports/04_Final/W19_EECS598_FinalReport.pdf).

## Results

The primary results of our project are documented in [this report](reports/04_Final/W19_EECS598_FinalReport.pdf). Some of our major findings are below.

### Finding 1: Higher-Order Networks Exhibit Meaningful Clusters

![Communities in First-Order vs. Higher-Order Networks](https://raw.githubusercontent.com/bdice/advanced-data-mining-project/master/reports/04_Final/figs/FON_vs_HON.png)

Shown are the communities identified using InfoMap for (a) a first order network and (b) a higher-order network. Only one community (the largest) is shown for the first order network, while the 5 largest communities are shown for the HON. Data is a snapshot from 2011 Q1. Node colors indicate communities, and sizes correspond to flow volume, defined in the text. Labels indicate three letter IATA airport codes.

### Finding 2: Airline Mergers Do Not Significantly Change Network Structure

![Data from Delta-Northwest Merger, 2010](https://raw.githubusercontent.com/bdice/advanced-data-mining-project/master/reports/04_Final/figs/DL_NW_merger.png)

Shown are data for (a) 2004 Q3 and (b) 2013 Q1. Delta (DL) and Northwest (NW) officially merged in 2010 Q1. Interestingly, we observe that this merger did not lead to major structural changes in the networks. Community numbers in the headers correspond to the order in which they were found by the InfoMap algorithm, and are not otherwise meaningful. For each community, the associated pie chart indicates the carriers represented, weighted by flow through the community. In the maps, node color and size indicates flow in the community (see colorbar). Labels indicate three letter IATA airport codes.

### Finding 3: Airport PageRanks Show Seasonal Dependence

![Seasonal Changes in First-Order PageRank](https://raw.githubusercontent.com/bdice/advanced-data-mining-project/master/reports/04_Final/figs/seasonality_map.png)

Shown above is seasonality of U.S. airports, as measured by PageRank in the first order network. Node colors correspond to the PageRank seasonality correlation coefficient described in the text and indicated on the colorbar. Node sizes correspond to PageRank in 2018 Q3, and labels indicate three letter IATA airport codes.

## Airline Data Workflow

The primary source code of this project is in the subdirectory `airline_data`. This contains code which fetches ticket information from the [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/Tables.asp?DB_ID=125) and analyzes it using PySpark, HON, and InfoMap [1, 2, 3].

Exploratory data analysis was performed in Jupyter notebooks. These notebooks, included in [`airline_data/exploration/`](airline_data/exploration/), were used to prototype code, collect intermediate results, and visualize the data.

The workflow was written as a series of operations in a [_signac-flow_](https://github.com/glotzerlab/signac-flow) project, described below. These operations include data cleaning, processing, analysis, and visualization tasks.

In addition to Jupyter notebooks, the data space was visualized using [_signac-dashboard_](https://github.com/glotzerlab/signac-dashboard), which enabled rapid inspection of data plots generated by the _signac-flow_ project.

### Installation

Use the requirements file (`pip install -U -r requirements.txt`) to acquire project dependencies.

Some notes:

- It was necessary to use Python 2 for compatibility with Spark 2.2 on the flux-hadoop cluster.
- The dependency `cartopy` is much easier to install via `conda` than `pip`. It is only needed for generating map visualizations.

### Running the workflow

Run `python init.py` to create the _signac_ data space.

Run `python project.py run --progress --show-traceback` to execute the workflow. The operations below are performed independently for each _signac_ job, corresponding to one quarter of data. All file paths are relative to the job workspace directory.

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
19. `plot_first_order_communities`: Plots each community with more than 0.5% of the network's flow, saved in `first_order_community_plots/first_order_community_*.png`.
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

### Sample Data

The directory [`airline_data/sample_data_2011Q1/`](airline_data/sample_data_2011Q1/) contains most of the files generated by this workflow for the data from 2011 Q1. See the [sample data README](airline_data/sample_data_2011Q1/README.md) for example images and additional information.

## References

1. Xu, J., Wickramarathne, T. L., & Chawla, N. V. (2016). Representing higher-order dependencies in networks. _Science Advances_, _2_(5), e1600028. [Publication](https://doi.org/10.1126/sciadv.1600028), [GitHub repository](https://github.com/xyjprc/hon).
2. Bureau of Transportation Statistics, Office of Airline Information. Airline Origin and Destination Survey (DB1B). [Data Source](https://www.transtats.bts.gov/DatabaseInfo.asp?DB_ID=125).
3. Rosvall, M., Esquivel, A. V, Lancichinetti, A., West, J. D., & Lambiotte, R. (2014). Memory in network flows and its effects on spreading dynamics and community detection. _Nature Communications_, _5_, 4630. [Publication](https://doi.org/10.1038/ncomms5630), [Website](http://www.mapequation.org/), [GitHub repository](https://github.com/mapequation/infomap).
4. Adorf, C. S., Dodd, P. M., Ramasubramani, V., & Glotzer, S. C. (2018). Simple data and workflow management with the _signac_ framework. _Comput. Mater. Sci._, _146_(C):220-229, 2018. [Publication](https://doi.org/10.1016/j.commatsci.2018.01.035), [Website](https://signac.io), [GitHub repository](https://github.com/glotzerlab/signac).
5. Koutra, D. [EECS 598 W19 course webpage](http://web.eecs.umich.edu/~dkoutra/courses/W19_598/).

# Advanced Data Mining Project
Bradley Dice, Shannon Moran, Dan McCusker
- Jian Xu, Thanuka L. Wickramarathne, Nitesh V. Chawla. [Representing higher-order dependencies in networks](http://advances.sciencemag.org/content/advances/2/5/e1600028.full.pdf). Science Advances 2, no. 5 (2016): e1600028. [GitHub repository](https://github.com/xyjprc/hon)
- [EECS 598 W19 course webpage](http://web.eecs.umich.edu/~dkoutra/courses/W19_598/)

## Airline Data Acquisition

This section refers to the code in the subdirectory `airline_data`, which fetches ticket information from the [Airline Origin and Destination Survey (DB1B)](https://www.transtats.bts.gov/Tables.asp?DB_ID=125).

Use the requirements file (`pip install -U -r requirements.txt`) to get the dependencies for these data acquisition scripts.

Run `python init.py` to create the data space.

Run `python project.py run` to execute the workflow:

1. Fetches data with `urllib.request`.
2. Removes `readme.html` from all zip files (having an extra file in the zip breaks `pandas`).
3. Reads dataset sizes (rows, columns) from each zip file with `pandas` and saves that metadata to the job document.

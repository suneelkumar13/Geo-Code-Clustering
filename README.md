# Geo-Code-Clustering
Geo Location Clustering Using K-Means Algorithm 

## Files description

1) #### DBpedia.ipynb
      - This is the pre-processing and visualization code used to extract & plot relevent information from the lat_longs.txt data.
2) #### Mobilenet.ipynb
      - This is the pre-processing and visualization code used to extract & plot relevent information from the devicestatus.txt data.
3) #### Syntheticdata.ipynb
      - This is the pre-processing and visualization code used to extract & plot relevent information from the sample_geo.txt data.    
4) #### Generic.py 
      - This file is used to run a spark job.
      - It takes input file name, output file name, distance calculation type and k (Number of clusters) as command line arguments.
5) #### ExampleVisualization.ipynb
      - This file has to code to visualize the entire process of cluster formation. 
      - The input file is the processed file with longitude and latitude data.
      - K can be varied depending on the requirement. 
      - This example shows the entire visualization of forming 5 clusters on pre-processed Synthetic data.
      -  Convergence distance = 5km is used to reduce the number of plots
6) #### big_data.py
      - A larger dataset of size 343.6 MB with 324405 geo-points (as of 3rd December 2020) was used to form 6 geo-clusters.
      - Data was obtained from https://www.kaggle.com/austinreese/craigslist-carstrucks-data. 
      - The spark job was submited on an Amazon EMR master node. 
      - The command line arguments are path of S3 input data location and s3 output file location.
7) #### big_data_visualization.ipynb
      - This is a visualization code for only the 5 clusters obtained by applying the algorithm on the used car data with k=5.
      - The output files on s3 was downloaded to local and visualized using geopandas.
8) #### final_report.pdf
      - The final report documents
         * Details about the data
         * Explination of the algorithm used to form clusters
         * Implications of applying the algorithm on different data.
         * Runtime analysis - using different k size, Haversine & Euclidian measures, with and without caching.
         * Big data application (Used car dataset) and its results. 
      


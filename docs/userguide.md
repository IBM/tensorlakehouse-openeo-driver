# User Guide

## Tutorials
We recommend the following notebooks to learn more about openEO:
- [HLSS30.ipynb](../tutorials/HLSS30.ipynb) shows how to pull HLSS30 data from COS and compute temporal aggregation
- [ERA5-hbase.ipynb](../tutorials/ERA5-hbase.ipynb) shows how to pull ERA5 data 
- [local_connection.ipynb](../tutorials/local_connection.ipynb) shows how to load local netcdf file as openEO datacube, clip area of interest and compute temporal aggregation 

### Setup

1. Create a python virtualenv (python 3.11.7 is the recommended version). 
2. Clone the repository:
```
git clone https://github.com/IBM/tensorlakehouse-openeo-driver.git
``` 

3. Go to
```
cd tensorlakehouse-openeo-driver/tutorials
```

4. Install dependencies:
```
pip install -r tutorial_requirements.txt
```

5. Run jupyter notebook or jupyter lab
```
jupyter lab notebooks
```
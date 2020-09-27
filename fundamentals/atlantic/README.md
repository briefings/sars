### Python Environment

In preparation for Docker, etc.


```bash
  conda create --prefix .../atlantic
  conda activate atlantic

  conda install -c anaconda python=3.7.7 
  conda install -c anaconda geopandas
  conda install -c anaconda requests
  conda install -c anaconda pywin32 jupyterlab nodejs
  conda install -c anaconda dask
  conda install -c anaconda python-graphviz
  conda install -c anaconda statsmodels
```

<br>

In relation to requirements.txt

```markdown
    pip freeze -r docs/filter.txt > requirements.txt
```

The file [filter.txt](./docs/filter.txt) summarises the directly installed packages.  Hence, [filter.txt](./docs/filter.txt) is used to create a demarcated [requirements.txt](requirements.txt).  Note:

* Do not include `python-graphviz`, `pywin32`, & `nodejs` in `filter.txt`
* `geopandas` installs `numpy` & `pandas`
* `requests` installs `urllib3`

<br>

Later: 

```bash
    conda install -c anaconda pytest coverage pytest-cov pylint
```

and perhaps ` pyyaml`, for testing & conventions purposes
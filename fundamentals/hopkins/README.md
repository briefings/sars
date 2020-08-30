### Python Environment

In preparation for Docker, etc.


```bash
  conda create --prefix .../hopkins
  conda activate hopkins

  conda install -c anaconda python=3.7.7 
  conda install -c anaconda geopandas
  conda install -c anaconda requests
  conda install -c anaconda pywin32 jupyterlab nodejs
  conda install -c anaconda dask python-graphviz
```

<br>

In relation to requirements.txt

```markdown
    pip freeze -r docs/filter.txt > requirements.txt
```

The file [filter.txt](./docs/filter.txt) summarises the directly installed packages.  Hence, [filter.txt](./docs/filter.txt) is used to create a demarcated [requirements.txt](requirements.txt).  Note:

* `python-graphviz`, `pywin32` & `nodejs` cannot be included in `filter.txt`
* `geopandas` installs `numpy` & `pandas`
* `requests` installs `urllib3`

<br>

Later: 

```bash
    conda install -c anaconda pytest coverage pytest-cov pylint
```

and perhaps ` pyyaml`, for testing & conventions purposes.

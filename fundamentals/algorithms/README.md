## Algorithms

For simple data structuring.

<br>
<br>

### Development Notes

The environment:

```bash
conda create --prefix .../algorithms

  conda install -c anaconda python=3.7.7
  conda install -c anaconda dask
  conda install -c anaconda python-graphviz
  conda install -c anaconda pywin32 jupyterlab nodejs
  conda install -c anaconda statsmodels
```

The requirements document is created via

````shell
pip freeze -r docs/filter.txt > requirements.txt
````

Note that the packages `pywin32`, `nodejs`, and `python-graphviz` are excluded from filter.txt, and

* dask installs
  * numpy
  * pandas
  
* python-graphviz installs
  * graphviz

  <br>
<br>
<br>
<br>

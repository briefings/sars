## Populations

Temporary notes for Populations.  Usage notes outlined within the [examples](https://colab.research.google.com/github/briefings/sars/blob/develop/fundamentals/populations/notebooks/examples.ipynb) notebook

<br>
<br>

### Environments

```bash
conda create --prefix .../Anaconda3/envs/populations python=3.7.7
conda activate populations
conda install -c anaconda dask
conda install -c anaconda python-graphviz
conda install -c anaconda pywin32 jupyterlab nodejs
```

* `dask` installs `numpy` and `pandas`
* Do not include `python-graphviz`, `pywin32`, `nodejs` in `filter.txt`

<br>
<br>

### References

Write more about `https://www2.census.gov/programs-surveys/popest/datasets/`.  For example, about

* `https://www2.census.gov/programs-surveys/popest/datasets/{segment}/state/detail/SCPRC-EST{year}-18+POP-RES.csv`

<br>
<br>
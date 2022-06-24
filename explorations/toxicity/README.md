<br>

## Toxicity

Merges the latest [Hopkins](https://github.com/briefings/sars/tree/develop/fundamentals/hopkins) county level 
data and the [NATA](https://www.epa.gov/national-air-toxics-assessment/2014-nata-assessment-results) toxicity scores.

<br>

### Development Notes

<br>

The environment is [miscellaneous](https://github.com/briefings/energy#development-notes).  In relation to requirements.txt

```markdown
    pip freeze -r docs/filter.txt > requirements.txt
```

The file [filter.txt](./docs/filter.txt) summarises the directly installed packages.  Hence, [filter.txt](./docs/filter.txt) is used to create a demarcated [requirements.txt](requirements.txt).  Note:

* Do not include `python-graphviz`, `pywin32`, & `nodejs` in `filter.txt`

<br>

### References

* [shutil.move](https://docs.python.org/3.8/library/shutil.html#shutil.move)
* [Dask DataFrame API](https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.from_pandas)
* [Creating a dask DataFrame](https://docs.dask.org/en/latest/dataframe-create.html)
* [Graphs](https://docs.dask.org/en/latest/graphviz.html)
* [Dask Scheduler](https://docs.dask.org/en/latest/scheduler-overview.html)
* [Managing Packages via Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-pkgs.html)


<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>
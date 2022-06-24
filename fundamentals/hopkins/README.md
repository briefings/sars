<br>

## Johns Hopkins Data

./fundamentals/hopkins/apply.sh

<br>

### Development Notes

The environment is [miscellaneous](https://github.com/briefings/energy#development-notes). In relation to requirements.txt

```markdown
    pip freeze -r docs/filter.txt > requirements.txt
```

The file [filter.txt](./docs/filter.txt) summarises the directly installed packages.  Hence, [filter.txt](./docs/filter.txt) is used to create a demarcated [requirements.txt](requirements.txt).  Note:

* `python-graphviz`, `pywin32` & `nodejs` cannot be included in `filter.txt`
* `geopandas` installs `numpy` & `pandas`
* `requests` installs `urllib3`

<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>
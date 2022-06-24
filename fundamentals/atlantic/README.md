## The Atlantic's COVID Tracking Project

./fundamentals/atlantic/apply.sh

<br>

### Development Notes

<br>

Environment: [miscellaneous](https://github.com/briefings/energy#development-notes)

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
<br>

<br>
<br>

<br>
<br>

<br>
<br>

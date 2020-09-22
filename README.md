# SARS

<br>

This project is focused on measures, metrics, trends, and models for understanding and anticipating the spread of coronavirus disease 2019 (COVID-2019).  The measures, metrics, and models are explored/developed via SARS-CoV-2 (Severe Acute Respiratory Syndrome Coronavirus 2) field tests data; tests data and measures thereof.  The country in focus is the United States, and the data sources are

* [The COVID Tracking Project](https://covidtracking.com)
* [Johns Hopkins University & Medicine](https://github.com/CSSEGISandData/COVID-19)
* [United States Census Bureau](https://www.census.gov/en.html)
* [United States Environment Protection Agency](https://www.epa.gov)

<br>
<br>

## Blogger

In progress.  The graphs and tables of results, alongside explanatory notes and methods, are embedded in the pages of a blog project; URL upcoming.  A few highlights are outlined here.

<br>

### Distributions Graphs

* [Atlantic](https://nbviewer.jupyter.org/github/briefings/sars/blob/develop/graphs/spreads/pages/atlantic.html) <br> Raw state level spreads [The COVID Tracking Project]
* [Atlantic/100k](https://nbviewer.jupyter.org/github/briefings/sars/blob/develop/graphs/spreads/pages/atlanticscaled.html) <br> Per capita, i.e., population scaled, state level spreads [The COVID Tracking Project]
* [Hopkins](https://nbviewer.jupyter.org/github/briefings/sars/blob/develop/graphs/spreads/pages/hopkins.html) <br> Raw state level spreads [Johns Hopkins]
* [Hopkins/100K](https://nbviewer.jupyter.org/github/briefings/sars/blob/develop/graphs/spreads/pages/hopkinsscaled.html) <br> Per capita, i.e., population scaled, state level spreads [Johns Hopkins]

<br>

### Graphs of Measures & Metrics
* [Tableau Graphs](https://public.tableau.com/profile/c.a.6464#!/) <br> A variety of interactive graphs.  Thus far, state level graphs; county level graphs are now being developed.

<br>

### Scripts

```css
	.sidebar-container .widget li{
		font-size:14px;
		line-height:normal;
		padding: 5px;
		margin-left: 1px;
	}
```

<br>
<br>


## Projects

* [atlantic](./fundamentals/atlantic) <br/> For retrieving the COVID Tracking Project

* [hopkins](./fundamentals/hopkins) <br/> For retrieving & structuring Johns Hopkins' data

* [algorithms](./fundamentals/algorithms) <br/> A mix of measures & metrics algorithms; algorithms are continuously added.

* [populations](./fundamentals/populations) <br/> For retrieving populations data.

* [spreads](./graphs/spreads) <br/> Candle stick distributions graphs; via HighCharts/JavaScript

#!/bin/bash

git commit fundamentals/atlantic/notebooks/dispersions.ipynb -m "Update fundamentals/atlantic/notebooks/dispersions.ipynb"
git commit fundamentals/atlantic/notebooks/prospects.ipynb -m "Update fundamentals/atlantic/notebooks/prospects.ipynb"
git commit fundamentals/atlantic/notebooks/trends.ipynb -m "Update fundamentals/atlantic/notebooks/trends.ipynb"

git commit fundamentals/atlantic/warehouse/curves* -m "Latest fundamentals/atlantic/warehouse/curves*"
git commit fundamentals/atlantic/warehouse/capita* -m "Latest fundamentals/atlantic/warehouse/capita*"
git commit fundamentals/atlantic/warehouse/candles/ -m "Latest fundamentals/atlantic/warehouse/candles/"

git commit fundamentals/atlantic/notebooks/warehouse/trends/ -m "Latest fundamentals/atlantic/notebooks/warehouse/trends/"
git commit fundamentals/atlantic/notebooks/warehouse/prospects/ -m "Latest fundamentals/atlantic/notebooks/warehouse/prospects/"
git commit fundamentals/atlantic/notebooks/warehouse/dispersions/ -m "Latest fundamentals/atlantic/notebooks/warehouse/dispersions/"

git commit fundamentals/atlantic/warehouse/accumulations.csv -m "Latest fundamentals/atlantic/warehouse/accumulations.csv"
git commit fundamentals/atlantic/warehouse/baselines.csv -m "Latest fundamentals/atlantic/warehouse/baselines.csv"
git commit fundamentals/atlantic/warehouse/candles.zip -m "Latest fundamentals/atlantic/warehouse/candles.zip"

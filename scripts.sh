#!/bin/bash

# Atlantic's COVID Tracking Project
cp fundamentals/atlantic/warehouse/candles/*.json graphs/spreads/data/atlantic

cp fundamentals/atlantic/warehouse/gazetteer.csv graphs/statistics/atlantic
cp fundamentals/atlantic/warehouse/increases.csv graphs/statistics/atlantic

# Hopkins
cp -r fundamentals/hopkins/warehouse/county/candles/ graphs/spreads/data/hopkins/county/
cp -r fundamentals/hopkins/warehouse/state/candles/ graphs/spreads/data/hopkins/state/
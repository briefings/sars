var Highcharts;
var optionSelected;
var dropdown = $('#option_selector');
var url = 'https://raw.githubusercontent.com/briefings/sars/develop/graphs/spreads/references/atlantic.json';


$.getJSON(url, function (data) {

    $.each(data, function (key, entry) {
        dropdown.append($('<option></option>').attr('value', entry.desc).text(entry.name));
    });

    // Load the first Option by default
    var defaultOption = dropdown.find("option:first-child").val();
    optionSelected = dropdown.find("option:first-child").text();

    // Generate
    generateChart(defaultOption);

});


// Dropdown
dropdown.on('change', function(e){

    $('#option_selector_title').remove();

    // Save name and value of the selected option
    optionSelected = this.options[e.target.selectedIndex].text;
    var valueSelected = this.options[e.target.selectedIndex].value;

    //Draw the Chart
    generateChart(valueSelected);
});


// Generate graphs
function generateChart(fileNamekey){

    $.getJSON('https://raw.githubusercontent.com/briefings/sars/develop/graphs/spreads/data/atlantic/'+fileNamekey+'.json', function (data) {

        // https://api.highcharts.com/highstock/plotOptions.series.dataLabels
        // https://api.highcharts.com/class-reference/Highcharts.Point#.name
        // https://api.highcharts.com/highstock/tooltip.pointFormat


        // split the data set into ohlc and medians
        var ohlc = [],
            medians = [],
            numbers = [],
            cumulative = [],
            dataLength = data.length,
            groupingUnits = [[
                'day',                         // unit name
                [1]                            // allowed multiples
            ]],
            i = 0;

        for (i; i < dataLength; i += 1) {

            ohlc.push([
                data[i][0], // the date
                data[i][2], // lower q   open
                data[i][5], // upper w  high
                data[i][1], // lower w  low
                data[i][4] // upper q  close
            ]);

            medians.push({
                x: data[i][0], // the date
                y: data[i][3] // median
            });

            numbers.push({
                x: data[i][0], // date
                y: data[i][6]  // counts
            });

            cumulative.push({
                x: data[i][0], // date
                y: data[i][8]  // cumulative counts
            });

        }

        Highcharts.setOptions({
            lang: {
                thousandsSep: ','
            }
        });

        // Draw a graph
        Highcharts.stockChart('container0003', {

            rangeSelector: {
                selected: 1,
                verticalAlign: 'top',
                floating: false,
                inputPosition: {
                    x: 0,
                    y: 0
                },
                buttonPosition: {
                    x: 0,
                    y: 0
                },
                inputEnabled: true,
                inputDateFormat: '%Y-%m-%d'
            },

            chart: {
                zoomType: 'x'
                // borderWidth: 2,
                // marginRight: 100
            },

            title: {
                text: 'Distributions of: ' + optionSelected
            },

            subtitle: {
                text: 'U.S.A.: The States, Washington D.C., & Puerto Rico <br/> <p> Data Source: C.T.P.</p>'
            },

            time: {
                // timezone: 'Europe/London'
            },

            credits: {
                enabled: false
            },

            legend: {
                enabled: true
                // align: 'middle',
                // layout: 'vertical',
                // verticalAlign: 'bottom',
                // y: 10,
                // x: 35
            },

            yAxis: [{
                labels: {
                    align: 'left',
                    x: 9
                },
                title: {
                    text: optionSelected,
                    x: 0
                },
                min: 0,
                height: '46%',
                lineWidth: 2,
                resize: {
                    enabled: true
                }
            },{
                labels: {
                    align: 'left',
                    x: 9
                },
                title: {
                    text: 'Daily Totals',
                    x: 0
                },
                top: '49%',
                height: '23%',
                offset: 0,
                lineWidth: 2
            },
            {
                labels: {
                    align: 'left',
                    x: 9
                },
                title: {
                    text: 'Continuous',
                    x: 0
                },
                top: '75%',
                height: '23%',
                offset: 0,
                lineWidth: 2
            }
            ],

            plotOptions:{
                series: {
                    turboThreshold: 4000
                }
            },

            tooltip: {
                split: true,
                dateTimeLabelFormats: {
                    millisecond:"%A, %e %b, %H:%M:%S.%L",
                    second:"%A, %e %b, %H:%M:%S",
                    minute:"%A, %e %b, %H:%M",
                    hour:"%A, %e %b, %H:%M",
                    day:"%A, %e %B, %Y",
                    week:"%A, %e %b, %Y",
                    month:"%B %Y",
                    year:"%Y"
                }

            },

            series: [{
                type: 'candlestick',
                name: 'Distribution of ' + optionSelected,
                data: ohlc,
                dataGrouping: {
                    units: groupingUnits,
                    dateTimeLabelFormats: {
                        millisecond: ['%A, %e %b, %H:%M:%S.%L', '%A, %b %e, %H:%M:%S.%L', '-%H:%M:%S.%L'],
                        second: ['%A, %e %b, %H:%M:%S', '%A, %b %e, %H:%M:%S', '-%H:%M:%S'],
                        minute: ['%A, %e %b, %H:%M', '%A, %b %e, %H:%M', '-%H:%M'],
                        hour: ['%A, %e %b, %H:%M', '%A, %b %e, %H:%M', '-%H:%M'],
                        day: ['%A, %e %b, %Y', '%A, %b %e', '-%A, %b %e, %Y'],
                        week: ['Week from %A, %e %b, %Y', '%A, %b %e', '-%A, %b %e, %Y'],
                        month: ['%B %Y', '%B', '-%B %Y'],
                        year: ['%Y', '%Y', '-%Y']
                    }
                },
                tooltip: {
                    pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b><br/>' +
                        'Upper Whisker: {point.high:,.2f}<br/>' +
                        'Upper Quartile: {point.close:,.2f}<br/>' +
                        'Lower Quartile: {point.open:,.2f}<br/>' +
                        'Lower Whisker: {point.low:,.2f}' + '<br/>'
                }
            },
                {
                    type: 'spline',
                    name: 'Median',
                    data: medians,
                    color: '#6B8E23',
                    yAxis: 0,
                    dataGrouping: {
                        units: groupingUnits
                    },
                    tooltip: {
                        pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b>: ' +
                            '{point.y:,.2f}<br/>'
                    }
                },
                {
                    type: 'spline',
                    name: 'The Day\'s Total',
                    data: numbers,
                    color: '#EDC948',
                    yAxis: 1,
                    dataGrouping: {
                        units: groupingUnits
                    },
                    tooltip: {
                        pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b>: ' +
                            '{point.y}<br/>'
                    }

                },
                {
                    type: 'spline',
                    name: 'Continuous Sum',
                    data: cumulative,
                    color: '#800000',
                    yAxis: 2,
                    dataGrouping: {
                        units: groupingUnits
                    },
                    tooltip: {
                        pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b>: ' +
                            '{point.y}<br/>'
                    }

                }


            ],
            responsive: {
                rules: [{
                    condition: {
                        maxWidth: 800
                    },
                    chartOptions: {
                        rangeSelector: {
                            inputEnabled: false
                        }
                    }
                }]
            }
        });

    }).fail(function() {
        console.log("Missing");
        $('#container0003').empty();
    });

}



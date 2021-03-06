var Highcharts;
var optionSelected;
var dropdown = $('#option_selector');
var url = 'https://raw.githubusercontent.com/briefings/sars/develop/graphs/spreads/assets/menu/atlanticcurrently.json';


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
dropdown.on('change', function (e) {

    $('#option_selector_title').remove();

    // Save name and value of the selected option
    optionSelected = this.options[e.target.selectedIndex].text;
    var valueSelected = this.options[e.target.selectedIndex].value;

    //Draw the Chart
    generateChart(valueSelected);
});


// Generate graphs
function generateChart(fileNamekey) {

    $.getJSON('https://raw.githubusercontent.com/briefings/sars/develop/fundamentals/atlantic/warehouse/candles/' + fileNamekey + '.json', function (data) {

        // https://api.highcharts.com/highstock/plotOptions.series.dataLabels
        // https://api.highcharts.com/class-reference/Highcharts.Point#.name
        // https://api.highcharts.com/highstock/tooltip.pointFormat


        // split the data set into ohlc and medians
        var ohlc = [],
            medians = [],
            maxima = [],
            numbers = [],
            cumulative = [],
            dataLength = data.length,
            groupingUnits = [[
                'day',   // unit name
                [1]      // allowed multiples
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

            maxima.push({
                x: data[i][0], // the date
                y: data[i][6] // maximum
            });

            numbers.push({
                x: data[i][0], // date
                y: data[i][7]  // counts
            });
            
        }

        Highcharts.setOptions({
            lang: {
                thousandsSep: ','
            }
        });

        // Draw a graph
        Highcharts.stockChart('container0100', {

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
                text: '<p>U.S.A.: The States, Washington D.C., Puerto Rico</p> <br/> ' +
                    '<p><b>Data Source</b>: The COVID Tracking Project</p>'
            },

            time: {
                // timezone: 'Europe/London'
            },

            credits: {
                enabled: false
            },

            legend: {
                enabled: true,
                width: 550,
                x: 10
                // align: 'middle',
                // layout: 'vertical',
                // verticalAlign: 'bottom',
                // y: 10,
                // x: 35
            },

            caption: {
                // verticalAlign: "top",
                text: '<p>Herein, each candlestick illustrates the spread of 52 points: 50 states, Washington D.C., and Puerto Rico.  Each day\'s point is ' +
                    'a measure, e.g., the number of patients ' + optionSelected + '  </p>'
            },

            exporting: {
                buttons: {
                    contextButton: {
                        menuItems: [ 'viewFullscreen', 'printChart', 'separator',
                            'downloadPNG', 'downloadJPEG', 'downloadPDF', 'downloadSVG' , 'separator',
                            'downloadXLS', 'downloadCSV']
                    }
                }
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
                height: '53%',
                lineWidth: 2,
                resize: {
                    enabled: true
                }
            }, {
                labels: {
                    align: 'left',
                    x: 9
                },
                title: {
                    text: 'Daily Totals',
                    x: 0
                },
                top: '60%',
                height: '37%',
                offset: 0,
                lineWidth: 2
            }

            ],

            plotOptions: {
                series: {
                    turboThreshold: 4000
                }
            },

            tooltip: {
                split: true,
                dateTimeLabelFormats: {
                    millisecond: "%A, %e %b, %H:%M:%S.%L",
                    second: "%A, %e %b, %H:%M:%S",
                    minute: "%A, %e %b, %H:%M",
                    hour: "%A, %e %b, %H:%M",
                    day: "%A, %e %B, %Y",
                    week: "%A, %e %b, %Y",
                    month: "%B %Y",
                    year: "%Y"
                }

            },

            series: [{
                type: 'candlestick',
                name: optionSelected,
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
                    name: 'Maxima',
                    data: maxima,
                    color: '#A08E23',
                    visible: false,
                    yAxis: 0,
                    dataGrouping: {
                        units: groupingUnits
                    },
                    tooltip: {
                        pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b>: ' +
                            '{point.y}<br/>'
                    }
                },
                {
                    type: 'column',
                    name: 'The Day\'s Total',
                    data: numbers,
                    color: '#000000',
                    yAxis: 1,
                    dataGrouping: {
                        units: groupingUnits
                    }
                    /*tooltip: {
                        pointFormat: '<span style="color:{point.color}">\u25CF</span> <b> {series.name} </b>: ' +
                            '{point.y}<br/>'
                    }*/

                }
            ],
            responsive: {
                rules: [{
                    condition: {
                        maxWidth: 700
                    },
                    chartOptions: {
                        rangeSelector: {
                            inputEnabled: false
                        }
                    }
                }]
            }
        });

    }).fail(function () {
        console.log("Missing");
        $('#container0100').empty();
    });

}



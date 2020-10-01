var Highcharts;
var optionSelected;
var seriesOptions = [];
var dropdown = $('#option_selector');
var url = 'https://raw.githubusercontent.com/briefings/sars/develop/graphs/risks/references/menu/respiratory/pollutant.json';


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

    $.getJSON('https://raw.githubusercontent.com/briefings/sars/develop/fundamentals/risks/correlations/warehouse/respiratory/pollutant/' + fileNamekey + '.json', function (risk) {

        // https://api.highcharts.com/highstock/tooltip.pointFormat
        // https://jsfiddle.net/gh/get/library/pure/highcharts/highcharts/tree/master/samples/highcharts/demo/bubble
        // https://api.highcharts.com/highcharts/tooltip.headerFormat
        // https://www.highcharts.com/demo/stock/compare


        // split
        var i = 0;
        var status;

        for (i; i < risk.length; i += 1) {

            status = i === 18;

            seriesOptions[i] = {
                name: risk[i].name,
                visible: status,
                data: risk[i].data
            };

        }

        Highcharts.setOptions({
            lang: {
                thousandsSep: ','
            }
        });


        Highcharts.chart('container0003', {

            chart: {
                type: 'scatter', // plotBorderWidth: 0,
                zoomType: 'xy'
            },

            credits: {
                enabled: false
            },

            legend: {
                enabled: true,
                x: 50
            },

            title: {
                text: 'Hazard & Deaths/100K [C] by County'
            },

            subtitle: {
                text: 'Source: <a href="https://www.epa.gov/national-air-toxics-assessment/2014-nata-assessment-results">EPA</a> ' +
                    'and <a href="https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series">JH</a>'
            },

            xAxis: {
                gridLineWidth: 1,
                title: {
                    text: 'deaths per 100K (continuous)'
                },
                labels: {
                    format: '{value}'
                },
                plotLines: [{
                    color: 'black',
                    dashStyle: 'dot',
                    width: 2,
                    value: 650,
                    label: {
                        rotation: 0,
                        y: 15,
                        style: {
                            fontStyle: 'italic'
                        },
                        text: 'Temporary vertical line ...'
                    },
                    zIndex: 3
                }]
            },

            yAxis: {
                startOnTick: false,
                endOnTick: false,
                title: {
                    text: 'hazard index'
                },
                labels: {
                    format: '{value}'
                },
                maxPadding: 0.2,
                plotLines: [{
                    color: 'black',
                    dashStyle: 'dot',
                    width: 2,
                    value: 1,
                    label: {
                        align: 'right',
                        style: {
                            fontStyle: 'italic'
                        },
                        text: 'Temporary horizontal line ...',
                        x: -10
                    },
                    zIndex: 3
                }]
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

            tooltip: {
                headerFormat: '<span style="font-size: 13px; color:{point.color}">\u25CF {point.key}, {series.name}</span>',
                pointFormat: '<br/><p><span style="color: white">Gap</span></p><p><br/>' +
                    'Deaths/100K [C]: {point.x:,.2f}<br/>' +
                    'Hazard Index: {point.y:,.2f}<br/></p>',
                style: {
                    fontSize: "11px"
                }
            },

            plotOptions: {
                series: {
                    dataLabels: {
                        enabled: false
                    },

                    turboThreshold: 4000
                }
            },

            series: seriesOptions

        });


    }).fail(function () {
        console.log("Missing");
        $('#container0003').empty();
    });


}
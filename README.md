# Weather Simulator Game

This fun project contains a simple weather simulator for the whole globe.
The idea of how it works is based on the [Game of Life](https://en.wikipedia.org/wiki/Conway%27s_Game_of_Life),
where life in every cell has some initial state and is affected by its neighbours and changed on every iteration.
Instead of rules for life, it contains rules for how the weather is affected. And instead of square grid,
the cells are based on latitude and longitude to easily represent the globe.
Thinking of the cells as graph has an additional benefit of being able to zoom in
in the areas of interest and zoom out where not required.

I'm using Spark GraphX to implement it, simply because I wanted to try it out,
and also so it can scale out in the future if required.

## Running the app

The sample seed data located in `seed-data` folder spans the globe and zooms in onto the North America
with 16 named weather stations. Weather predictions for a particular day could look like:

    SFO|37.75768|-122.50764|2017-06-19T21:42:03Z|Sunny|16.0|1017.7|55
    MEX|19.411406|-99.75886|2017-06-19T23:14:03Z|Sunny|22.2|1018.4|37
    DFW|32.82035|-97.011536|2017-06-19T23:22:03Z|Rain|25.2|1010.2|48
    YWG|49.853737|-97.292305|2017-06-19T23:22:03Z|Sunny|11.3|1022.8|51
    YVR|49.256107|-123.193954|2017-06-19T21:38:03Z|Sunny|10.1|1031.8|53
    DTW|42.352627|-83.23929|2017-06-20T00:18:03Z|Sunny|17.2|1030.2|49
    LAX|34.02018|-118.691925|2017-06-19T21:58:03Z|Rain|23.3|1013.3|62
    DEN|39.764256|-104.99519|2017-06-19T22:54:03Z|Sunny|16.3|1017.0|47
    MSY|30.020687|-90.02359|2017-06-19T23:50:03Z|Sunny|21.6|1030.5|50
    BOS|42.31329|-71.61747|2017-06-20T01:06:03Z|Rain|18.7|995.8|71
    JAX|30.344692|-82.000656|2017-06-20T00:22:03Z|Sunny|24.7|1015.1|43
    YYC|51.012783|-114.35434|2017-06-19T22:14:03Z|Rain|10.3|1012.4|86
    YQB|46.85792|-71.48616|2017-06-20T01:06:03Z|Rain|13.1|1010.8|79
    PHX|33.6051|-112.40524|2017-06-19T22:22:03Z|Sunny|24.3|1015.1|38
    SLC|40.776524|-112.06057|2017-06-19T22:22:03Z|Rain|16.0|1006.4|63
    ORD|41.833393|-88.012344|2017-06-19T23:58:03Z|Sunny|15.8|1024.0|48

By default, the simulation predicts the weekly weather for the whole year.
The results are stored in [results\weather-stations-results.psv](results\weather-stations-results.psv).
To run it with default options:

    sbt run

Or, by giving it custom parameters:

    sbt "run-main eu.fastdata.ws.WeatherSimulator [numberOfWeeksToPredict] [outputFileName] [inputEdgesFileName] [inputVerticesFileName]"

There are some simple tests in the project which can be run with:

    sbt test


## TODOs

There a plenty of them. The most interesting however, would be to plot the weather stations reports on a map like Google Maps
(using their lat and lon coordinates). A sequence of those maps would visually represent changing weather conditions over time.
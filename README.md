# blaseball-new-event-parser
New Event Parser for the New Era of Blaseball
Series of three python files that output to csv
# get_from_pusher_fillgaps.py
This file pulls from SIBR's API2 and creates a table, filling empty gaps in the pitcher, pitcher id, batter, batter id, balls, and strikes columns with the previous values, and outputs it into a csv
# pusher_to_cleanevents.py
This file pulls the csv from the previous file, and merges multi-line events into single events, and pushes it to a new csv
# cleanevents-typer.py
This file pulls the csv from the previous file, and parses the events into different event types, and pushes it to a new csv

To keep track of a single set of data, each file asks for the input of a pipeline name, which will track the file along.

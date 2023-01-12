import pandas as pd

pd.set_option('display.max_columns', None)

pipeline_label = str(input("pipeline label: "))

data = pd.read_csv(pipeline_label + '_apioutput.csv')

game_ids = data['gameId'].unique()
game_event_types = {'batter up',
                    'ball',
                    'strike',
                    'walk',
                    'strikeout',
                    'foul',
                    'single',
                    'double',
                    'triple',
                    'home run',
                    'out',
                    'fielders choice',
                    'double play',
                    'triple play',
                    'black hole',
                    'end of inning'}

# just put things into game events using displayOrder

events = pd.DataFrame(columns=['gameId',
                               'displayText',
                               'pitcher',
                               'pitcherId',
                               'batter',
                               'batterId',
                               'balls',
                               'strikes',
                               'eventType'])

prev_displayOrder = -1
multiline_holder = ""
is_multiline = False
for index, row in data.iterrows():
    if row['balls'] != 'None':
        data.at[index, 'balls'] = int(row['balls'])
    if row['strikes'] != 'None':
        data.at[index, 'strikes'] = int(row['strikes'])
    if type(row['displayText']) == str:
        if is_multiline:
            # handling for multiline events
            # always add line, check if end of multilinesd
            multiline_holder += (row['displayText'] + "\n")
            if type(data.at[min(index + 1, len(data) - 1), 'displayText']) == str:
                if "steps up to bat" in data.at[min(index + 1, len(data) - 1), 'displayText'] or \
                        "End of the " in data.at[min(index + 1, len(data) - 1), 'displayText'] or \
                        "Game Over" in data.at[min(index + 1, len(data) - 1), 'displayText'] or \
                        "Play Ball!" in data.at[min(index + 1, len(data) - 1), 'displayText']:
                    # end of multiline, add to table
                    is_multiline = False
                    if index % 1000 == 0:
                        print("adding ", end="")
                        print(multiline_holder)
                    event_temp = pd.DataFrame([[row['gameId'],
                                                multiline_holder,
                                                row['pitcherName'],
                                                row['pitcherId'],
                                                row['batterName'],
                                                row['batterId'],
                                                row['balls'],
                                                row['strikes'],
                                                "NONETYPE"]], columns=events.columns)
                    events = pd.concat([events, event_temp], ignore_index=True)
                    multiline_holder = ""

        else:
            # might be new event
            # skip play ball
            if "Play Ball!" not in row['displayText']:
                prev_displayOrder = row['displayOrder']
                # check if start of multiline
                if data.at[min(index + 1, len(data) - 1), 'displayOrder'] == row['displayOrder']:
                    multiline_holder = row['displayText'] + "\n"
                    is_multiline = True
                elif row['displayText'][-3:] == "...":
                    multiline_holder = row['displayText'] + "\n"
                    is_multiline = True
                else:
                    is_multiline = False
                    if index % 1000 == 0:
                        print("adding", end="")
                        print(row['displayText'])
                    event_temp = pd.DataFrame([[row['gameId'],
                                                row['displayText'],
                                                row['pitcherName'],
                                                row['pitcherId'],
                                                row['batterName'],
                                                row['batterId'],
                                                row['balls'],
                                                row['strikes'],
                                                "NONETYPE"]], columns=events.columns)
                    events = pd.concat([events, event_temp], ignore_index=True)

events.to_csv(pipeline_label + "_cleanedevents.csv")

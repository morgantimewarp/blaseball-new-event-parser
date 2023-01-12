import pandas as pd
import re

pd.set_option('display.max_columns', None)

pipeline_label = input("pipeline label: ")

game_events = pd.read_csv(pipeline_label + "_cleanedevents.csv")

game_events['balls'] = (pd.to_numeric(game_events['balls'], errors='coerce').fillna(0))
game_events['strikes'] = (pd.to_numeric(game_events['strikes'], errors='coerce').fillna(0))

game_ids = game_events['gameId'].unique()
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
                    'sacrifice'
                    'horizon drop',
                    'end of inning',
                    'end of game'}
prev_balls = 0
prev_strikes = 0
for index, row in game_events.iterrows():
    # adding to ball count
    if prev_balls < row['balls']:
        if re.search(r"(Ball|ball)", row['displayText']) is not None:
            game_events.at[index, 'eventType'] = 'ball'
        else:
            print("cannot read this ball: ", end="")
            print(row['displayText'])
    # adding to strike count
    elif prev_strikes < row['strikes']:
        if re.search(r"(Strike|strike)", row['displayText']) is not None:
            game_events.at[index, 'eventType'] = 'strike'
        elif re.search(r"((foul|Foul).*(ball|tip|off|back))|(out.of.play)", row['displayText']) is not None:
            game_events.at[index, 'eventType'] = 'foul'
        else:
            print("cannot read this strike: ", end="")
            print(row['displayText'])
    elif re.search(r"Game.Over", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'end of game'
    elif re.search(r"BURP", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'horizon drop'
    elif re.search(r"End.of.the.*of.the", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'end of inning'
    # batter up
    elif re.search(r"steps.up.to.bat", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'batter up'
    elif row['strikes'] == 2:
        if re.search(r"((foul|Foul).*(ball|tip|off|back))|(out.of.play)", row['displayText']) is not None:
            game_events.at[index, 'eventType'] = 'foul'
        else:
            print("cannot read this foul: ", end="")
            print(row['displayText'])
    # walk
    elif re.search(r"(takes.their.base|Ball.4|(earns|draws).a.walk)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'walk'
    # strikeout
    elif re.search(r"(strikes.*out)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'strikeout'
    elif re.search(r"Home Run!", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'home run'
    elif re.search(r"((T|t)riple|hustles.all.the.way.to.third)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'triple'
    elif re.search(r"(Double)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'double'
    elif re.search(r"(Single)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'single'
    elif re.search(r"causes.multiple.outs", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'double play'
    elif re.search(r"((f|F)ielder.*choice|is.tagged.out.at)", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'fielders choice'
    elif re.search(r"((f|F)ly.out|(g|G)roundout|forced.out.at|gets.the.out|"
                   r"make.*.catch|simple.catch|secures.it|corrals.it|sacrifice)", row['displayText']) is not None:
        if re.search(r"(advances|scores|sacrifice)", row['displayText']) is not None:
            game_events.at[index, 'eventType'] = 'sacrifice'
        else:
            game_events.at[index, 'eventType'] = 'out'
    # ball / strike edge cases
    elif re.search(r"(S|s)trike.*[0-9]-[0-9]", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'strike'
    elif re.search(r"(B|b)all.*[0-9]-[0-9]", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'ball'
    elif re.search(r"(F|f)oul.*[0-9]-[0-9]", row['displayText']) is not None:
        game_events.at[index, 'eventType'] = 'foul'
    else:
        print("still can't parse: ", end = "")
        print(row['displayText'])
    prev_balls = row['balls']
    prev_strikes = row['strikes']

    if index % 1000 == 0:
        print("finished: ", end = "")
        print(row['displayText'])

game_events.to_csv(pipeline_label + "_typed_events.csv")
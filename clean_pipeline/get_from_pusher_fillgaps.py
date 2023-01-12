import json
import requests

gamelist = requests.get("https://api2.sibr.dev/mirror/games")
games_ids = json.loads(gamelist.text)

data = []

games = 0

for i in games_ids:
    if (i.get("complete") == True):
        response = requests.get("https://api2.sibr.dev/chronicler/v0/game-events?game_id=" + str(i.get("id")))
        gamedat = json.loads(response.text)
        p_pitcher = ""
        p_pitcher_id = ""
        p_batter = ""
        p_batter_id = ""
        p_strikes = ""
        p_balls = ""
        for line in gamedat:
            linedat = line.get("data")
            state = linedat.get("changedState")
            pitcher = state.get("pitcher")
            batter = state.get("batter")
            parsedLine = []
            parsedLine.append(line.get("game_id"))
            parsedLine.append(linedat.get("displayTime"))
            parsedLine.append(linedat.get("displayOrder"))
            parsedLine.append(linedat.get("displayText"))
            try:
                parsedLine.append(pitcher.get("name"))
                parsedLine.append(pitcher.get("id"))
                p_pitcher = pitcher.get("name")
                p_pitcher_id = pitcher.get("id")
            except:
                parsedLine.append(p_pitcher)
                parsedLine.append(p_pitcher_id)
            try:
                parsedLine.append(batter.get("name"))
                parsedLine.append(batter.get("id"))
                p_batter = batter.get("name")
                p_batter_id = batter.get("id")
            except:
                parsedLine.append(p_batter)
                parsedLine.append(p_batter_id)
            try:
                parsedLine.append(state.get("balls"))
                p_balls = state.get("balls")
            except:
                parsedLine.append(p_balls)
            try:
                parsedLine.append(state.get("strikes"))
                p_strikes = state.get("strikes")
            except:
                parsedLine.append(p_strikes)
            data.append(parsedLine)
        if games % 10 == 0:
            print(str(games) + " games processed")
        games = games + 1

with open(input("pipeline label: ") + '_apioutput.csv', 'w+', encoding='utf-8') as outf:
    print('gameId,displayTime,displayOrder,displayText,pitcherName,pitcherId,batterName,batterId,balls,strikes',
          file=outf)
    for t in data:
        print('"' + str(t[0]) + '","' + str(t[1]) + '","' + str(t[2]) + '","' + str(t[3]) + '","' + str(
            t[4]) + '","' + str(t[5]) + '","' + str(t[6]) + '","' + str(t[7]) + '","' + str(t[8]) + '","' + str(
            t[9]) + '"', file=outf)

outf.closed

from decimal import Decimal 
import json
import boto3



def load_musics(musics, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('music')
    for music in musics:
        artist = music['artist']
        year = int(music['year'])
        music['year'] = year
        title = music['title']
        print("Adding music:",artist, year, title)
        table.put_item(Item=music)


if __name__ == '__main__':
    with open("a1.json") as json_file:
        music_list = json.load(json_file, parse_float=Decimal)
    load_musics(music_list['songs'])
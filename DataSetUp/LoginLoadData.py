from decimal import Decimal 
import json
import boto3



def load_musics(logins, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('login')
    for user in logins:
        print("Adding user:", user)
        table.put_item(Item=user)


if __name__ == '__main__':
    with open("login.json") as json_file:
        login_list = json.load(json_file, parse_float=Decimal)
    load_musics(login_list)
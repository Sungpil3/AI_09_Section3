import json
for filename in ['partsEngine', 'partsHandle', 'partsKit', 'partsWheel']:
    with open(f'../metadata/{filename}.json', "r", encoding='UTF8') as st_json:
        json_data = json.load(st_json)
    for dict in json_data:
        dict['Key'] = str(dict['Key'])
        dict['id'] = dict.pop("Key")
        dict['name'] = dict.pop("Name")
    with open(f'../metadata/{filename}.json', "w", encoding='UTF8') as json_file:
        json.dump(json_data,json_file)

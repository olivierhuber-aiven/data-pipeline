import csv
import json


def make_json(csv_file_path, json_file_path):

    data = []

    with open(csv_file_path, encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for rows in csv_reader:
            data.append(rows)

    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json_file.write(json.dumps(data, indent=4))


make_json("products.csv", "products.json")

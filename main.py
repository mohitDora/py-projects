
import json
import requests
from pathlib import Path

# Specify the path to your JSON file
# file_path = 'C:/Users/Public/data.json'
home_dir = Path.home()
print(home_dir)
file_path = home_dir / 'data' / 'data.json'

# Open the JSON file and load the data
try:
    with open(file_path, 'r') as file:
        data = json.load(file)
except FileNotFoundError:
    print(f"The file {file_path} does not exist.")
except json.JSONDecodeError:
    print(f"Error decoding JSON from the file {file_path}.")


#destructuring data
# url=data["url"]

#fetch API
# response = requests.get(url)
# response = response.json()

# Print the loaded data
# print(response)
print(data)
input()
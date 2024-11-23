import requests

# URL of your Flask route
url = "http://141.94.27.12//bonne_adresse/upload_file/"

# File to upload
file_path = "zeop.xlsx"

# Open the file and send it as part of the POST request
with open(file_path, 'rb') as file:
    response = requests.post(url, files={'file': file})

# Print the response
print(response.status_code)
print(response.text)

import urllib, json, requests

with urllib.request.urlopen("https://geo.fcc.gov/api/census/area?lat=42.3295&lon=-71.0826&format=json") as url:
    data = json.loads(url.read().decode())
    print(data)

print(json.dumps(data, indent=4, sort_keys=True))

print(data['results'][0]['state_fips'])
print(data['results'][0]['county_fips'])
print(data['results'][0]['county_name'])

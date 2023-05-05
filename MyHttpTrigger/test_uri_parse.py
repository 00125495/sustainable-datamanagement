from urllib.parse import urlparse
import pandas as pd

# Example dataframe with 'uri' column
df = pd.DataFrame({'uri': ['https://rgazurefunctionleara88b.blob.core.windows.net:443/demo/test_del.txt?_=1683083219565&sv=2021-12-02&ss=bqtf&srt=sco&sp=rwdlacuptfxiy&se=2023-05-03T11:06:58Z&sig=XXXXX', 'https://rgazurefunctionleara88b.blob.core.windows.net:443/?restype=service&comp=properties&_=1683083144764&sv=2021-12-02&ss=bqtf&srt=sco&sp=rwdlacuptfxiy&se=2023-05-03T09:54:05Z&sig=XXXXX']})

# Define lambda function to extract components
def extract_components(row):
    parsed_uri = urlparse(row['uri'])
    print(parsed_uri)
    return pd.Series({
        'prototype': parsed_uri.scheme,
        'hostname': parsed_uri.hostname,
        'path': parsed_uri.path,
        'port': parsed_uri.port,
        'query': parsed_uri.query,
        'fragment': parsed_uri.fragment,
        'params': parsed_uri.params,
    })

# Apply lambda function to 'uri' column and add new columns to dataframe
df[['prototype', 'hostname', 'path', 'port', 'query', 'fragment', 'params']] = df.apply(extract_components, axis=1)

# Print resulting dataframe
print(df.head())




import pandas as pd
import json

# Example DataFrame
df = pd.DataFrame({'property': ['{"accountName": "rgazurefunctionleara88b", "etag": "\\"0x8DB4B79592EA47E\\"", "serviceType": "blob", "objectKey": "/rgazurefunctionleara88b/demo", "lastModifiedTime": "5/3/2023 1:54:36 AM", "metricResponseType": "Success", "serverLatencyMs": 1, "requestHeaderSize": 335, "responseHeaderSize": 380, "tlsVersion": "TLS 1.2"}']})

# Convert JSON string to dictionary and extract keys
keys = set()
for row in df['property']:
    d = json.loads(row)
    keys.update(d.keys())

print(keys)

import requests
import json

# Define the Azure services you are using
services = ['Virtual Machines', 'Storage Accounts', 'Networking']

# Get the Azure Resource Manager endpoint URL
url = 'https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Consumption/usageDetails?api-version=2019-10-01&$filter='

# Define your Azure Resource Manager API credentials
headers = {
    'Authorization': 'Bearer {accessToken}',
    'Content-Type': 'application/json'
}

# Define the carbon intensity factor for your region
carbon_intensity_factor = 0.000703 # kgCO2/kWh

# Loop through the Azure services and calculate the CO2 emissions
total_emissions = 0
for service in services:
    # Filter the usage data for the current service
    filter_query = "properties/usageStart ge '2023-01-01' and properties/usageEnd le '2023-03-01' and properties/instanceName eq '{}'"
    response = requests.get(url + filter_query.format(service), headers=headers)
    usage_data = json.loads(response.text)['value']

    # Calculate the energy consumption and CO2 emissions for the current service
    energy_consumption = sum([float(d['properties']['pretaxCost']) for d in usage_data])
    co2_emissions = energy_consumption * carbon_intensity_factor
    total_emissions += co2_emissions

    # Print the results for the current service
    print('{} energy consumption: {} kWh'.format(service, energy_consumption))
    print('{} CO2 emissions: {} kg'.format(service, co2_emissions))
    print()

# Print the total CO2 emissions for your Azure environment
print('Total CO2 emissions: {} kg'.format(total_emissions))

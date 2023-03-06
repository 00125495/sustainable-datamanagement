# sustainable-datamanagement
This code uses the Azure Consumption API to retrieve usage data for the specified Azure services and calculates the energy consumption and CO2 emissions for each service based on the carbon intensity factor for your region. The total CO2 emissions for your Azure environment are then printed at the end of the script.

Note that you will need to replace the {subscriptionId} and {accessToken} placeholders in the url and headers variables with your own Azure Resource Manager API credentials. Additionally, you may need to modify the date range in the filter_query variable to match the time period for which you want to calculate CO2 emissions.

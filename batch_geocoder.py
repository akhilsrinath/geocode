"""
Python script for batch geocoding of addresses using the Google Geocoding API. This script allows for massive lists of addresses to be geocoded. Addresses for geocoding are specified in a list of strings "addresses". In the script, addresses came from a CSV with a column "Address". After every 500 successful geocode operations, a temporary file with results is recorded in case of script failure/connection loss, etc.

"""

import pandas as pd
import requests
import logging
import time

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

#------------------ CONFIGURATION -------------------------------

# Set Google API key
API_KEY = "AIzaSyCa4DIK5GuDAkl2wz5PDCZrI_haa-_--Hk"
BACKOFF_TIME = 30

# Set output file
output_filename = "batch_geocoding_results.csv"
# Set input filename
input_filename = "loc_info.csv"
# Specify column name in input data that contains addresses
address_column_name = "Address"
# Return Full Google Results? If True, full JSON results from Google are included in output
RETURN_FULL_RESULTS = True

#------------------ DATA LOADING --------------------------------

# Read the data to a Pandas Dataframe
data = pd.read_csv(input_filename, encoding='latin')
if address_column_name not in data.columns:
    raise ValueError("Missing Address column in input data")

# Form a list of addresses that need to be geocoded
addresses = data[address_column_name].tolist()
addresses = addresses[10000:20001]

#------------------	FUNCTION DEFINITIONS ------------------------

def get_google_results(address, api_key=None, return_full_response=False):

"""
    Get geocode results from Google Maps Geocoding API.
    Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.
    @param address : string address as accurate as possible
    @param api_key : String API key if possible
    @param return_full_response : Boolean if you'd like to return the full response from Google

"""
    # Set up geocoding url
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    if api_key is not None:
        geocode_url = geocode_url + "&key={}".format(api_key)

    # Ping Google for the results :
    results = requests.get(geocode_url)
    # Results will be JSON format - convert dict using requests functionality
    results = results.json()

    # If there are no results or an error, return empty results
    if len(results["results"]) == 0 :
        output = {
            "address_components": None,
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
    else:
        answer = results['results'][0]
        output = {
            "address_components" : answer.get("address_components"),
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components')
                                  if 'postal_code' in x.get('types')])
        }

    # append some other details :
    output['input_string'] = address
    output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if return_full_response is True :
        output['response'] = results

    return output

#------------------ PROCESSING LOOP -----------------------------

test_result = get_google_results("zakir nagar sosouth east delhidelhi110025", API_KEY, RETURN_FULL_RESULTS)

geocode_result = {'status': 'starting'}

# Create list to hold results
results = []

# geocode each address
for address in addresses:
    # While address geocoding is not finished
    geocoded = False
    while geocoded is not True:
        # Geocode address
        try:
            geocode_result = get_google_results(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
    # If API limit is reached, back off for a while and try again
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes
            geocoded = False
        else:
            # If we're ok with API use, save the results
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            results.append(geocode_result)
            geocoded = True

    # Print status every 100 addresses :
    if len(results) % 100 == 0:
    	logger.info("Completed {} of {} address".format(len(results), len(addresses)))

    # Every 500 addresses, save progress to file(in case of a failure so you have something!)
    if len(results) % 20 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

# All done
logger.info("Finished Geocoding all addresses")
# Write results to csv
pd.DataFrame(results).to_csv(output_filename, encoding="latin")

from constants import LAPOSTE_API

import requests

def get_city_zip_data(**kwargs):
    """
    This function calls laposte API to retrieve city zip data
    Input:
        **kwargs => named parameters that are going to be used as request parameters
    Return:
        response["results"]: list => returns the response list containing the city zip data
    """
    url = LAPOSTE_API
    if kwargs:
        first = True
        for key, value in kwargs.items():
            if first:
                url = f"{url}?{key}={value}"
                first = False
            else:
                url = f"{url}&{key}={value}"

    response = requests.get(url=url).json()
    return response["results"]

def get_city_by_zip(zip:str):
    """
    This function searches for the city name corresponding to the zip code provided in laposte API
    Input:
        zip : str => zip code 
    Return:
        city : str : name of the city if found in the api
    """
    res = get_city_zip_data(size=1,select="code_postal,nom_de_la_commune",q=f'{{code_postal:"{zip}"}}',q_fields='code_postal')
    if len(res) > 0:
        city = res[0]["nom_de_la_commune"]
        return city
    return None

def get_zip_by_city(city:str):
    """
    This function searches for the zip code corresponding to the city name provided in laposte API
    Input:
        city : str => city name
    Return:
        zip : str : zip code if found in the api
    """
    res = get_city_zip_data(size=1,select="code_postal,nom_de_la_commune",q=f'{{nom_de_la_commune:"{city}"}}',q_fields='nom_de_la_commune')
    if len(res) > 0:
        zip = int(res[0]["code_postal"])
        return zip
    return None

def get_address_by_coordinates(lat : float, lon : float):
    """
    This function calls openstreetmap API to convert latitude and longitude coordiantes to addresses.
    Input:
        lat : float => latitude value
        lon : float => longitude value
    Return:
        address : str => Converted address
    """
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&accept-language=fr&zoom=18'
    try:
        result = requests.get(url=url)
        result_json = result.json()
        address = result_json['display_name'].split(',')
        return address
    except Exception as e:
        print(f"Error: {e}")
        return None
	

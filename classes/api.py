import requests


class Call_api:
    def __init__(self):
        self.per_page = '200'

    def get_data(self, page):
        """""access the API, to get the data"""""
        url = f"https://api.worldbank.org/v2/country/ARG;BOL;BRA;CHL;COL;ECU;GUY;PRY;PER;SUR;URY;VEN/indicator/NY.GDP.MKTP.CD?format=json&page={page}&per_page={self.per_page}"

        response = requests.request("GET", url)
        return response.json()

    def get_max_page(self, data):
        """""pass the result of api to collect the number of pages in the result of API"""""
        max_page = data[0]['pages']
        return max_page

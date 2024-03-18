import scrapy
import pandas as pd
from urllib.parse import quote
from scrapy.http import Response
import json


class MiamiDade(scrapy.Spider):
	name = "miamidade"


	def start_requests(self):
		urls = self.load_input()
		for url in urls[:1]:
			yield scrapy.Request(url, callback=self.parse)


	def parse(self, response: Response, **kwargs):
		data = json.loads(response.text)
		if data.get("Completed", False) and data.get("MinimumPropertyInfos"):
			folio = data.get("MinimumPropertyInfos")[0].get("Strap").replace("-",'')
			url = f"https://www.miamidade.gov/Apps/PA/PApublicServiceProxy/PaServicesProxy.ashx?Operation=GetPropertySearchByFolio&clientAppName=PropertySearch&folioNumber={folio}"
			yield scrapy.Request(url, callback=self.parse_property)


	def parse_property(self, response: Response):
		data = json.loads(response.text)
		if data:
			item = {
				"address": self.get_address(data),
				"owners": self.get_owners(data),
				"primary_land_use": f'{data.get("PropertyInfo", {}).get("DORCode")} {data.get("PropertyInfo", {}).get("DORDescription")}',
				"actual_area": data.get("PropertyInfo", {}).get("BuildingGrossArea"),
				"living_area": data.get("PropertyInfo", {}).get("BuildingHeatedArea"),
				"adjusted_area": data.get("PropertyInfo", {}).get("BuildingEffectiveArea"),
				"market_value": data.get("Taxable", {}).get("TaxableInfos", [{}])[0].get("SchoolTaxableValue"),
				"assessed_value": data.get("Taxable", {}).get("TaxableInfos", [{}])[0].get("CountyTaxableValue"),
			}
			return item


	@staticmethod
	def get_address(data):
		address = {
			"property_address": data.get("MailingAddress", {}).get("Address1"),
			"city": data.get("MailingAddress", {}).get("City"),
			"country": data.get("MailingAddress", {}).get("Country"),
			"state": data.get("MailingAddress", {}).get("State"),
			"zipcode": data.get("MailingAddress", {}).get("ZipCode"),
		}
		address['mailing_address'] = f"{address['property_address']} {address['city']}, {address['state']} {address['zipcode']}"
		return address


	@staticmethod
	def get_owners(data):
		owners = []
		for owner in data.get("OwnerInfos"):
			owners.append(owner.get("Name"))
		return owners


	@staticmethod
	def load_input():
		urls = []
		df = pd.read_csv("input.csv")
		for idx, row in df.iterrows():
			address = quote(f"{row['NUMBER']} {row['PREDIR']} {row['STNAME']} {row['STSUFFIX']}")
			url = f"https://www.miamidade.gov/Apps/PA/PApublicServiceProxy/PaServicesProxy.ashx?Operation=GetAddress&clientAppName=PropertySearch&myUnit=&from=1&myAddress={address}&to=200"
			urls.append(url)
		return urls
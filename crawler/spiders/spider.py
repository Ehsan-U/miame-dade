from typing import Dict
import scrapy
import pandas as pd
from urllib.parse import quote
from scrapy.http import Response
import json



class MiamiDade(scrapy.Spider):
	name = "miamidade"


	def start_requests(self):
		urls = self.load_input()
		for url in urls:
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
				"building_information": {
					"year_built": data.get("Building", {}).get("BuildingInfos", [{}])[0].get("Actual"),
					"actual_sqft": data.get("Building", {}).get("BuildingInfos", [{}])[0].get("GrossArea"),
					"living_sqft": data.get("Building", {}).get("BuildingInfos", [{}])[0].get("HeatedArea"),
					"calc_value": data.get("Building", {}).get("BuildingInfos", [{}])[0].get("DepreciatedValue"),
				}
			}
			url = f"https://miamidade.county-taxes.com/public/real_estate/parcels/{data.get('PropertyInfo', {}).get('FolioNumber').replace('-','')}"
			yield scrapy.Request(url, callback=self.parse_taxes, cb_kwargs={"item": item}, meta={
                "zyte_api_automap": True
            })


	def parse_taxes(self, response: Response, item: Dict):
		taxes = []
		installments_page = response.xpath("//tr[@class='year-footer']")
		bill_year = None

		for idx, row in enumerate(response.xpath("//table/tbody/tr[not(@class='d-table-row d-md-none')]"), start=1):
			sub_item = {}
			
			if installments_page:
				if row.xpath("self::node()[not(@class='installment')]"):
					if row.xpath("./th[@class='year-header']"):
						bill_year = row.xpath("./th[@class='year-header']/a[1]/text()").get()
					elif row.xpath("self::node()[@class='year-footer']"):
						status = row.xpath("./td[@class='label status']/text()").re_first("\w+")
						sub_item = {
							"bill": bill_year,
							"amount_due": row.xpath("./td[@class='label status']/preceding-sibling::td[@class]/text()").get('$0.00').strip(),
							"amount_paid": row.xpath("./td[@class='label status']/text()").re_first("[\d,\.$]+"),
							"status": status
						}
			else:
				status = row.xpath("./td[contains(@class, 'status')]/span[@class='label']/text()").get('').strip()
				sub_item = {
					"bill": row.xpath("./th/a[1]/text()").get(),
					"amount_due": row.xpath("./td[@class='balance']/text()").get('').strip(),
					"amount_paid": row.xpath("./td[contains(@class, 'status')]/span/following-sibling::text()").get('').strip(),
					"status": status
				}

			taxes.append(sub_item) if sub_item else None
			if len(taxes) == 3:
				break
		item['property_taxes'] = taxes
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
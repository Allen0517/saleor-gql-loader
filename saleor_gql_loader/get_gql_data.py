import xlrd
import os
import time
import string
import random
import json
from io import StringIO
from xlrd import open_workbook, cellname
from saleor_gql_loader import ETLDataLoader
from saleor_gql_loader.utils import graphql_request, graphql_multipart_request, override_dict, handle_errors, get_payload
from decouple import Config, RepositoryEnv


DOTENV_FILE = '/home/eric/sdep-ecommerce/.env'
# DOTENV_FILE = '/home/fytron/sdep-ecommerce/saleor/.env'
env_config = Config(RepositoryEnv(DOTENV_FILE))
ETL_SECRET_ID = env_config('ETL_SECRET_ID')

# Setup Excel
EXCEL_FILE_LOCATION = env_config('EXCEL_FILE_LOCATION')
EXCEL_FILE_NAME = env_config('EXCEL_FILE_NAME')

# Setup Excel Cols
# This method excepts the following data types for products
# Change nums if values are in different columns
NAME_COL = 0
SKU_COL = 1
PRICE_COL = 2
DESCRIPTION_COL = 4
WEIGHT_COL = 8
CATEGORY_COL = 11
IMAGE_COL = 12
SEO_TITLE_COL = 13
SEO_DESC_COL = 14

class ETLDataGetter(ETLDataLoader):
	def get_product(self, product_id):
		"""get_product.
		Parameters
		----------
		product_id : str
			product id required to query the product.
		Returns
		-------
		product : dict
			the product object.
		"""

		variables = {
			"id": product_id
		}

		# * Definition: product(id: ID, slug: String): Product
		query = """
			fragment TaxedMoneyFields on TaxedMoney {
				currency
				gross {
					amount
					localized
				}
				net {
					amount
					localized
				}
				tax {
					amount
					localized
				}
			}

			fragment TaxedMoneyRangeFields on TaxedMoneyRange {
				start {
					...TaxedMoneyFields
				}
				stop {
					...TaxedMoneyFields
				}
			}

			fragment ProductPricingFields on ProductPricingInfo {
				onSale
				discount {
					...TaxedMoneyFields
				}
				discountLocalCurrency {
					...TaxedMoneyFields
				}
				priceRange {
					...TaxedMoneyRangeFields
				}
				priceRangeUndiscounted {
					...TaxedMoneyRangeFields
				}
				priceRangeLocalCurrency {
					...TaxedMoneyRangeFields
				}
			}

			fragment ProductVariantFields on ProductVariant {
				id
				sku
				name
				stockQuantity
				isAvailable
				pricing {
					discountLocalCurrency {
						...TaxedMoneyFields
					}
					price {
						currency
						gross {
							amount
							localized
						}
					}
					priceUndiscounted {
						currency
						gross {
							amount
							localized
						}
					}
					priceLocalCurrency {
						currency
						gross {
							amount
							localized
						}
					}
				}
				attributes {
					attribute {
						id
						name
					}
					values {
						id
						name
						value: name
					}
				}
			}

			query get_product($id: ID!) {
				product(id: $id) {
					id
					seoTitle
					seoDescription
					name
					description
					descriptionJson
					publicationDate
					isPublished
					productType {
						id
						name
					}
					slug
					category {
						id
						name
					}
					updatedAt
					chargeTaxes
					weight {
						unit
						value
					}
					thumbnail {
						url
						alt
					}
					pricing {
						...ProductPricingFields
					}
					isAvailable
					basePrice {
						currency
						amount
					}
					taxType {
						description
						taxCode
					}
					variants {
						...ProductVariantFields
					}
					images {
						id
						url
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return response["data"]["product"]

	def update_product(self, product_id, product):
		"""update_product.
		Parameters
		----------
		product_id : str
			product id required to query the product.
		product : Product
			product with fields to update to
		Returns
		-------
		product : dict
			updates the product object.
		"""

		# define updated project obj from product to update from data
		updated_product = {
			"category": product["category"],
			"chargeTaxes": product["chargeTaxes"],
			"descriptionJson": product["descriptionJson"],
			"isPublished": product["isPublished"],
			"name": product["name"],
			"basePrice": product["basePrice"],
			"taxCode": "",
			"seo": {
				"title": product["seo"]["title"],
				"description": product["seo"]["description"]
			}
		}

		variables = {
			"id": product_id,
			"input": updated_product
		}

		# * Definition: product(id: ID, input: Product): Product
		query = """
			mutation productUpdate($id: ID!, $input: ProductInput!) {
				productUpdate(id: $id, input: $input) {
					product {
						id
						name
					}
					productErrors {
						field
						message
						code
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		errors = response["data"]["productUpdate"]["productErrors"]
		handle_errors(errors)

		return response["data"]["productUpdate"]["product"]["name"] + " was updated."


	def product_excel_import_all(self):
		# declare location of excel file to be imported
		location = open_workbook(EXCEL_FILE_LOCATION + EXCEL_FILE_NAME, 'r')
		sheet = location.sheet_by_index(0)
		num_rows_to_execute = 50

		# create a product type of car parts, save ID
		product_type_id = self.create_product_type(
			name = "Car Parts"
		)

		# ! @ERIC I removed the dictionary since it just made things really confusing in your method.
		# ! @ERIC What we should probably do, is query for ALL the categories and store them in here
		# ! so that new categories wont be created multiple times.
		# create categories list with all existing categories
		categories_list = self.query_all_categories()
		print(categories_list)
		# create dictionary to hold all the objects imported form excel sheet
		products = []

		# iterate over each row in the sheet, pass the variables gotten from each col
		for row in range(1, sheet.nrows)[:num_rows_to_execute]:
			if sheet.cell_value(row, NAME_COL):
				product_name = sheet.cell_value(row, NAME_COL)
				if "DEL THIS ITEM" in product_name:
					print("Product for deletion found. Skipping Product...")
					continue
			else:
				continue

			if sheet.cell_value(row, SKU_COL):
				product_sku = sheet.cell_value(row, SKU_COL)
			else:
				continue

			if sheet.cell_value(row, PRICE_COL):
				product_price = float(sheet.cell_value(row, PRICE_COL))
			else:
				continue

			if sheet.cell_value(row, DESCRIPTION_COL):
				product_description = self.get_description(sheet.cell_value(row, DESCRIPTION_COL))
			else:
				product_description = "This product has no description."

			if sheet.cell_value(row, WEIGHT_COL):
				product_weight = {
					'unit': 'LB',
					'value': float(sheet.cell_value(row, WEIGHT_COL))
				}
			else:
				product_weight = None

			# get and split categories into parent and child
			if sheet.cell_value(row, CATEGORY_COL):
				product_categories = sheet.cell_value(row, CATEGORY_COL).split('/')
			else:
				continue

			# start = time.perf_counter()
			product_category_id = self.deepest_id(product_categories, categories_list)
			# end = time.perf_counter()
			# print("TIME: {0}".format(end-start))

			product_image_url = sheet.cell_value(row, IMAGE_COL)

			# ! @ERIC Title must have at most 70 characters, this needs to be handled probably on the excel sheet side.
			# ! @ERIC Temporary code:::
			product_seo_title = sheet.cell_value(row, SEO_TITLE_COL)[:70]
			# product_seo_title = sheet.cell_value(row, SEO_TITLE_COL)
			product_seo_description = sheet.cell_value(row, SEO_DESC_COL)

			#  declare and initalize a product object to pass to the products dict
			product_object = {
				"product_name" : product_name,
				"product_sku" : product_sku,
				"product_description" : product_description,
				"product_price" : product_price,
				"product_weight" : product_weight,
				"product_category" : product_category_id,
				"product_image_url" : product_image_url,
				"product_seo_title" : product_seo_title,
				"product_seo_description" : product_seo_description,
				"product_category_id" : product_category_id
			}

			print("Created product object", product_name, "with SKU", product_sku)
			print("Found matching category ID", product_category_id)
			# add product obj to products dict
			products.append(product_object)

		print("Adding product objects to database")
		for product in products[:num_rows_to_execute]:
			product_obj = {
				'name': product["product_name"],
				'sku': product["product_sku"],
				'descriptionJson': product["product_description"],
				'chargeTaxes': True,
				'isPublished': True,
				'trackInventory': False,
				'category': product["product_category_id"],
				'basePrice': product["product_price"],
				'weight': product["product_weight"],
				'seo': {
					"title" : product["product_seo_title"],
					"description" : product["product_seo_description"]
				}
				# ? add to createProductImage Later
				# ? imageURL = product["product_image_url"],
			}

			try:
				product_id = self.create_product(product_type_id, **product_obj)
				print("Product", product["product_name"], "with SKU", product["product_sku"], "successfully added to database")
			except:
				print("Product with SKU: " + product["product_sku"] + " already exists. Updating Product...")
				update_id = self.get_product_by_sku(product["product_sku"])
				self.update_product(update_id, product_obj)
				print("Product with SKU", product["product_sku"], "successfully updated in the database")

	# ! @David This is the method you wrote, but with different variable names and comments
	# def get_deepest_child_id(self, product_categories, categories_dictionary, parent_id = None):
	# 	print(categories_dictionary)
	# 	# Iterate through all categories
	# 	for category in product_categories:
	# 		# If category exists in specified dictionary
	# 		if category in categories_dictionary.values():
	# 			# Iterate through children of found category
	# 			for child_category_dict in category['children']:
	# 				# search for next deepest category in child dicitonary
	# 				return get_deepest_child_id(product_categories[1:], child_category_dict, categories_dictionary['id'])
	# 			# if category has no children, return to create new category
	# 			return get_deepest_child_id(product_categories[1:], categories_dictionary, categories_dictionary['id'])
	# 		# If no category found in dictionary, create it and add to relative dictionary as a child
	# 		new_category_id = self.category_create(category, parent_id)
	# 		categories_dictionary['children'].append(self.create_category_dictionary(new_category_id, category))
	# 		# Search next category with the relative dictionary as the newly created dictionary
	# 		return get_deepest_child_id(product_categories[1:], categories_dictionary['children'][-1], new_category_id)
	# 	# If no categories left to search, return the ID of the deepest category
	# 	return parent_id

	def deepest_id(self, categories, categories_list, parent_id = None):
		# If no categories left to search, return the parent ID
		if not categories:
			return parent_id
		else:
			# For all categories in child category list
			for category in categories_list:
				# If one with matching name exists
				if categories[0] in category.values():
					return self.deepest_id(
						categories[1:],
						category['children'],
						category['id']
					)
			# If no matching child categories, create one
			print('No matching category found. Creating category \"' + categories[0] + '\"')
			new_cat_id = self.category_create(categories[0], parent_id)
			new_cat_dict = self.create_category_dictionary(new_cat_id, categories[0])
			categories_list.append(new_cat_dict)
			# Search on created category
			return self.deepest_id(
				categories[1:],
				categories_list[-1]['children'],
				new_cat_id
			)

	def create_category_dictionary(self, id, name, children_list = None):
		new_dict = {
			"id": id,
			"name": name,
			"children" : []
		}

		if children_list is not None:
		    new_dict["children"] = children_list

		return new_dict

	def get_product_by_sku(self, product_sku):
		"""get_product_by_sku.
		Parameters
		----------
		product_sku : str
			product sku to search for.
		Returns
		-------
		id : ID!
			ID of the product with the matching sku.
		"""

		variables = {
			"search": product_sku
		}

		query = """
			query products($search: String!) {
				products(first: 100, filter: {search: $search}) {
					edges {
						node {
							id
							variants {
								sku
							}
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return self.get_matching_sku_helper(response["data"]["products"], product_sku)

	def get_matching_sku_helper(self, products, product_sku):
		for product_edge in products["edges"]:
			for product_variants in product_edge["node"]["variants"]:
				if product_variants["sku"] == product_sku:
					return product_edge["node"]["id"]

	def get_category_by_name(self, category_name):
		"""get_product_by_sku.
		Parameters
		----------
		category_name : str
			category name to search for.
		Returns
		-------
		id : ID!
			ID of the category with the matching name.
		"""

		variables = {
			"search": category_name
		}

		query = """
			query categories($search: String!) {
				categories(first: 100, filter: {search: $search}) {
					edges {
						node {
							id
							name
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return self.get_category_by_name_helper(response["data"]["categories"], category_name)

	def get_category_by_name_helper(self, categories, category_name):
		for category_edge in categories["edges"]:
			if category_edge["node"]["name"] == category_name:
				return category_edge["node"]["id"]


	def category_create(self, name, parent_id = None):
	    """create a category
	    Parameters
	    ----------
	    **kwargs : dict, optional
	    overrides the default value set to create the category refer to
	    the productTypeCreateInput graphQL type to know what can be
	    overriden.
	    Returns
	    -------
	    id : str
		the id of the productType created.
	    Raises
	    ------
	    Exception
		when productErrors is not an empty list.
	    """

	    category = {
			"name" : name
		}

	    variables = {
		    "input": category,
	    }

	    if parent_id is not None:
		    variables["parent"] = parent_id

	    query = """
		    mutation createCategory($input: CategoryInput!, $parent: ID) {
			    categoryCreate(input: $input, parent: $parent) {
				    category {
					    id
				    }
				    productErrors {
					    field
					    message
					    code
				    }
			    }
		    }
	    """

	    response = graphql_request(
		    query, variables, self.headers, self.endpoint_url)

	    errors = response["data"]["categoryCreate"]["productErrors"]
	    handle_errors(errors)

	    return response["data"]["categoryCreate"]["category"]["id"]

	def query_all_categories(self):
	    """create a category
	    Parameters
	    ----------
		None
	    Returns
	    -------
	    list : list
		list of all the categories in the database.
	    Raises
	    ------
	    Exception
		when productErrors is not an empty list.
	    """

	    variables = {}

	    query = """
			query categories {
				categories(first: 100) {
					edges {
						node {
							name
							id
							children(first: 100){
								edges {
									node {
										name
										id
									}
								}
							}
							ancestors(first: 100){
								edges {
									node {
										name
										id
									}
								}
							}
						}
					}
				}
			}
		"""

	    response = graphql_request(
		    query, variables, self.headers, self.endpoint_url)

		# Return edges of all categories
	    return self.get_parent_categories(response["data"]["categories"]["edges"])

	def get_category_children(self, name):
		"""get_product_by_sku.
		Parameters
		----------
		category_name : str
			category name to search for.
		Returns
		-------
		list : List!
			List of child categories of category.
		"""

		variables = {
			"search": name
		}

		query = """
			query categories($search: String!) {
				categories(first: 100, filter: {search: $search}) {
					edges {
						node {
							id
							name
							children(first: 100) {
								edges {
									node {
										name
										id
										children(first: 100) {
											edges {
												node {
													name
													id
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return self.get_category_children_helper(response["data"]["categories"], name)

	def get_category_children_helper(self, categories, name):
		# Returns children list of exact category
		for category_edge in categories["edges"]:
			if category_edge["node"]["name"] == name:
				return category_edge["node"]["children"]["edges"]


	def get_parent_categories(self, categories):
		# Create list for categories with no parents
		parent_categories = []
		final_list = []
		# If category has no parents, add it to list
		for category in categories:
			if not category["node"]["ancestors"]["edges"]:
				# Add category to list
				parent_categories.append(category)
				# Add category to list in proper format
				new_list_entry = self.create_category_dictionary(category["node"]["id"], category["node"]["name"])
				final_list.append(new_list_entry)
		# Return the list of categories for the get_deepest_id method
		return self.create_categories_list(final_list, parent_categories)

	def create_categories_list(self, category_list, parent_categories):
		# For all categories in parent categories
		for category in parent_categories:
			# if category has child categories
			if category["node"]["children"]["edges"]:
				# For all child categories
				for child_category in category["node"]["children"]["edges"]:
					# Find matching parent
					for list_category in category_list:
						if category["node"]["name"] in list_category.values():
							# Create and add formatted entry to formatted parent
							new_list_entry = self.create_category_dictionary(
								child_category["node"]["id"],
								child_category["node"]["name"],
								self.get_category_children(child_category["node"]["name"])
							)
							list_category["children"].append(new_list_entry)
							# Call again with chilren list and child edges
							recursive_category_list = self.get_category_children(child_category["node"]["name"])
							self.create_categories_list(list_category["children"], recursive_category_list)
		return category_list

	def purge_products(self):

		edges = self.get_all_product_ids()
		ids = []

		for node in edges:
			ids.append(node["node"]["id"])
		
		variables = {
			"ids": ids
		}
	
		query = """
			mutation productBulkDelete($ids: [ID]!) {
				productBulkDelete(ids: $ids) {
					count
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return None

	def get_all_product_ids(self):
		variables = {}

		query = """
			query products {
				products(first: 100) {
					edges {
						node {
							id
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return response["data"]["products"]["edges"]

	def get_description(self, input_html):
		test_string = input_html

		replace_dictionary = {
			"<li>": "",
			"</li>": "",
			"<b>": "",
			"</b>": "",
			"<h2>": "",
			"</h2>": "",
			"<div>": "",
			"</div>": "",
			"<li>": "",
			"</li>": "",
			"<ul>": "",
			"</ul>": "",
			"<input type=\"button\" class=\"btn btn-primary\" onclick=\"location.href=\'http://sandiegoengineparts.com/p/submit-your-price\';\" value=\"Submit Your Bid\">": "",
			"<h2 style=\"font-size: 32px;\">Don't Like the Price?": "",
		}
		new_set = self.replace_all_helper(test_string, replace_dictionary)
		final_list = []
		for line in new_set.split("\n"):
			if not line.strip():
				continue
			line = line.strip()
			final_list.append(line)

		return self.get_description_string(final_list)

	def replace_all_helper(self, text, dic):
		for i, j in dic.items():
			text = text.replace(i, j)
		return text

	def get_description_string(self, lines_list):
		final_string = {
			"blocks": [],
			"entityMap": {}
		}
		for line in lines_list:
			temp_dict = {
				"key": ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(6)),
				"data": {},
				"text": f"{line}",
				"type": "unstyled",
				"depth": 0,
				"entityRanges":[],
				"inlineStyleRanges":[]
			}
			final_string["blocks"].append(temp_dict)
		return json.dumps(final_string, indent=4)


etl_data_getter = ETLDataGetter(ETL_SECRET_ID)
etl_data_getter.product_excel_import_all()

# print(etl_data_getter.get_description(None))

# ! To purge all products
# for x in range(100):
# 	etl_data_getter.purge_products()

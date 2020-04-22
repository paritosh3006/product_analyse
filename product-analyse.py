from functools import partial
from flask_cors import CORS
from flask import request, jsonify
import gdown
import flask
import io
import os
import pandas as pd
import json
import operator
from collections import defaultdict
import numpy as np
import logging

ops = { ">": operator.gt, "<": operator.lt, "==": operator.eq} 

url = 'https://drive.google.com/a/greendeck.co/uc?id=19r_vn0vuvHpE-rJpFHvXHlMvxa8UOeom&export=download'
#url = 'https://greendeck-datasets-2.s3.amazonaws.com/netaporter_gb_similar.json'

# Create Flask application
app = flask.Flask(__name__)
CORS(app)

# Contains a list of ids in json format
product_json = []

def init_files(dump_path = 'dumps/netaporter_gb.json'):
    
    if dump_path.split('/')[0] not in os.listdir():
        os.mkdir(dump_path.split('/')[0])
    if os.path.exists(dump_path):
        print('[INFO]: File exists already.')
    else:
        print('[INFO]: Downloading file...')
        gdown.download(url = url, output = dump_path, quiet=False)

def prepare_dataset(path = 'dumps/netaporter_gb.json'):
    neta_json_file = open(path, 'r', encoding='utf-8')
    with neta_json_file as fp:
        print('[INFO]: Processing file..')
        global product_json
        for product in fp.readlines():
            # Reading each json and storing into a list
            product_json.append(json.loads(product))

    if product_json != []:
        print('[INFO]: Processing file completed successfully')
    else:
        print('[INFO]: Processing file did not complete successfully.')

@app.route('/') 
def welcome():
    return 'Welcome to my app!'

def filter_me(filters):
    
    operand1 = [item['operand1'] for item in filters]
    operand2 = [item['operand2'] for item in filters]
    operator = [item['operator'] for item in filters]
    return operand1, operand2, operator

def discounted_products_list(data):

    query_type = data['query_type'] 
    filters = data['filters'] if 'filters' in data.keys() else None
#     logging.warning('Discounted Products list')
    if filters is not None:
        operand1, operand2, operator = filter_me(filters)
        result = defaultdict(list) # stores result
#         logging.warning(f'Filters are not empty. {query_type} {operand1}, {operand2}, {operator}')
        for idx in range(len(operand1)):
            # If user query for the discount
            if operand1[idx] == 'discount':
                # logging.warning('Discount')
                for item in product_json:
                    # logging.warning('Going through each json')
                    # parsing regular price and offer price
                    regular_price, offer_price = item['price']['regular_price']['value'], item['price']['offer_price']['value']
                    #  calculating discount
                    discount = (regular_price - offer_price) * 100 / regular_price
                    if ops[operator[idx]](discount, operand2[idx]):
                        # storing id if it satisfies the user given conditiion
                        result[query_type].append(item['_id']['$oid'])

            # If user query for the brand name
            elif operand1[idx] == 'brand.name':
                # logging.warning('Brand Name')
                for item in product_json:
                    # parsing and converting brand name to lowe case
                    brand_name = item['brand']['name'].lower()
                    # matching brand name with user given brand name
                    if ops[operator[idx]](brand_name, operand2[idx].lower()):
                        # storing id if it matches
                        result[query_type].append(item['_id']['$oid'])

            # If user query for the competition
            elif operand1[idx] == 'competition':
                # logging.warning('Competition')
                for item in product_json:
                    if 'similar_products' in item.keys():
                        # Looking for the competitor present or not
                        if operand2[idx] in item['similar_products']['website_results'].keys():
                            # storing id if the competitor is present
                            result[query_type].append(item['_id']['$oid'])

        if result != {}:
            # Returning the result to the user
            return jsonify(result)
        else:
            # Returning empty results if no results found
            return jsonify({query_type: [""]})
    else:
        # if filters are not present then returning empty result
        return jsonify({query_type: [""]})
                    
def discounted_products_count(data):

    query_type = data['query_type'] 
    filters = data['filters'] if 'filters' in data.keys() else None
    if filters is not None:
        operand1, operand2, operator = filter_me(filters)
        result = defaultdict(list)
        product_discount = []  # Stores product discount
        for idx in range(len(operand1)):
            # For discount
            if operand1[idx] == 'discount':
                for item in product_json:
                    # Parsing regular price and offer price
                    regular_price, offer_price = item['price']['regular_price']['value'], item['price']['offer_price']['value']
                    # Calculating discount
                    discount = (regular_price - offer_price) * 100 / regular_price
                    if ops[operator[idx]](discount, operand2[idx]):
                        # Storing discounts if they match the given constraint
                        product_discount.append(discount)

            # For brand name
            elif operand1[idx] == 'brand.name':
                    for item in product_json:
                        # Parsing regular price and offer price
                        regular_price, offer_price = item['price']['regular_price']['value'], item['price']['offer_price']['value']
                        # Calculating discount
                        discount = (regular_price - offer_price) * 100 / regular_price
                        brand_name = item['brand']['name'].lower()
                        if ops[operator[idx]](brand_name, operand2[idx].lower()):
                            # Storing discounts if they match the given constraint
                            product_discount.append(discount)

            # For competition
            elif operand1[idx] == 'competition':
                    for item in product_json:
                        if 'similar_products' in item.keys():
                            if operand2[idx] in item['similar_products']['website_results'].keys():
                                # Parsing regular price and offer price
                                regular_price, offer_price = item['price']['regular_price']['value'], item['price']['offer_price']['value']
                                # Calculating discount
                                discount = (regular_price - offer_price) * 100 / regular_price
                                product_discount.append(discount) # Storing discounts if they match the given constraint


        if product_discount != []:

            result[query_type.split('|')[0]] = len(product_discount)  # Number of discounts
            result[query_type.split('|')[1]] = round(np.mean(product_discount), 2)  # Average of discounts

            # Returning the result to the user
            return jsonify(result)
        else:
            # Returning empty results if no results found
            return jsonify({query_type.split('|')[0]: 0, query_type.split('|')[1]: 0})
    else:
        # if filters are not present then returning empty result
        return jsonify({query_type.split('|')[0]: 0, query_type.split('|')[1]: 0})

def expensive_list(data):
    query_type = data['query_type'] 
    filters = data['filters'] if 'filters' in data.keys() else None
    if filters is not None:
        operand1, operand2, operator = filter_me(filters)
        result = defaultdict(list)
        for idx in range(len(operand1)):
            if operand1[idx] == 'brand.name':
                for item in product_json:
                    if 'similar_products' in item.keys():
                        brand_name = item['brand']['name'].lower() # Parsing brand name and converting it to lower case
                        if ops[operator[idx]](brand_name, operand2[idx].lower()):
                                basket_price_NAP = item['price']['basket_price']['value'] # Parsing basket price for NAP
                                competitors = item['similar_products']['website_results'].keys() # Parsing competitors keys
                                competitions = item['similar_products']['website_results'] # Parsing competitors
                                # Parsing knn items, not storing if it's empty
                                knn_items = [competitions[competitor]['knn_items'] for competitor in competitors if competitions[competitor]['knn_items'] != []]
                                if knn_items != []:
                                    #  Parsing basket price for each competitor
                                    basket_price_comp = [knn_items[idx][0]['_source']['price']['basket_price']['value'] for idx in range(len(knn_items))]
                                    flag = False
                                    # Iterating over competitor basket price
                                    for price in basket_price_comp:
                                        # If any competitor price is greater than NAP basket price, setting flag as true
                                        if basket_price_NAP > price:
                                            flag = True
                                            break
                                    if flag:
                                        # Storing id if NAP basket price > competitor basket price
                                        result[query_type].append(item['_id']['$oid'])
        if result != {}:
            # Returning the result to the user
            return jsonify(result)
        else:
            # Returning empty results if no results found
            return jsonify({query_type: [""]})
    else:
        # if filters are not present then returning empty result
        return jsonify({query_type: [""]})

def competition_discount_diff_list(data):

    query_type = data['query_type'] 
    filters = data['filters'] if 'filters' in data.keys() else None
    if filters is not None:
        operand1, operand2, operator = filter_me(filters)
        result = defaultdict(list)

        for item in product_json:
            if 'similar_products' in item.keys():
                if operand2[1] in item['similar_products']['website_results'].keys():
                    # Parsing NAP offer price and competitor offer price
                    offer_price_NAP, offer_price_comp = item['price']['regular_price']['value'], item['similar_products']['website_results'][operand2[1]]['meta']['min_price']['offer']
                    # Calculating discount
                    discount = (offer_price_NAP - offer_price_comp) * 100 / offer_price_NAP
                    if ops[operator[0]](discount, operand2[0]):
                        result[query_type].append(item['_id']['$oid'])

        if result != {}:
            # Returning the result to the user
            return jsonify(result)
        else:
            # Returning empty results if no results found
            return jsonify({query_type: [""]})
    else:
        # if filters are not present then returning empty result
        return jsonify({query_type: [""]})
    
@app.route('/filter', methods=["POST"])

def request_from_client():
    data = request.get_json()

    # Performing actions based on query type
    if data['query_type'] == 'discounted_products_list':
        return discounted_products_list(data)
    elif data['query_type'] == 'discounted_products_count|avg_discount':
        return discounted_products_count(data)
    elif data['query_type'] == 'expensive_list':
        return expensive_list(data)
    elif data['query_type'] == 'competition_discount_diff_list':
        return competition_discount_diff_list(data)
    else:
        return jsonify({"error" : "Something went wrong"})
    
# RUN FLASK APPLICATION
if __name__ == '__main__':

    init_files('dumps/netaporter_gb.json') 
    
    # PREPARING DATASET
    prepare_dataset('dumps/netaporter_gb.json')
    
    # RUNNNING FLASK APP
    PORT = int(os.environ.get("PORT", 5000))
    app.run(debug=True, use_reloader=False, host = '0.0.0.0', port=PORT)

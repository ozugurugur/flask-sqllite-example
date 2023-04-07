from flask import Flask,jsonify,request
from flask_sqlalchemy import SQLAlchemy


import os
import ast
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
db = SQLAlchemy(app)

class Sellers(db.Model):
    sellerID = db.Column(db.String(50), primary_key=True)
    categoryID = db.Column(db.String(50), nullable=False)

class Orders(db.Model):
    orderID = db.Column(db.String(50), primary_key=True)
    itemIDs = db.Column(db.String(50))
    categoryIDs = db.Column(db.String(50))
    sellerIDs = db.Column(db.String(50))
    orderstatus = db.Column(db.String(50))
    price = db.Column(db.String(50))


def fetch_all_sellers_from_db():
    #query the sellers table and return as dictionary list
    sellers = Sellers.query.all()
    category_ids_list = [ s.categoryID for s in sellers]
    sellers_list = [{'sellerID':s.sellerID,'categoryID':s.categoryID} for s in sellers]
    return sellers_list,category_ids_list


def fetch_orders_with_given_category_from_db(category):

    #query the orders table and return values as dictionary list
    orders = Orders.query.all()
    orders_list = [{'orderID': o.orderID, 'itemIDs': o.itemIDs,
                    'categoryIDs': o.categoryIDs, 'sellerIDs': o.sellerIDs,
                    'orderstatus': o.orderstatus, 'price': o.price} for o in orders]


    #sqlite doesnt support array type columns, so its stored as string in db 
    #columns returned as lists -->
    output_orders_list=[]
    for order in orders_list:
        order['categoryIDs']= ast.literal_eval(order['categoryIDs'])
        order['itemIDs'] = ast.literal_eval(order['itemIDs'])
        order['sellerIDs'] = ast.literal_eval(order['sellerIDs'])
        order['price'] = ast.literal_eval(order['price'])


        #with given category from url, orders are filtered
        if category in order['categoryIDs']:
            output_orders_list.append(order)


    return output_orders_list

#ETL function
def calculate_each_sellers_gmv_with_category(orders_list,all_category_ids,expected_categoryId):
    sellers_gmv_dict = {}
    sellers_gmv_dict['sellers'] = {}
    category_list_dict = {category : 0 for category in all_category_ids}
    
    for order in orders_list:

        

        if order['orderstatus'] == 'INVALID':
            continue


        for sellerId in order['sellerIDs']:

            seller_index = order['sellerIDs'].index(sellerId)
            categoryId = order['categoryIDs'][seller_index]
            price = float(order['price'][seller_index])

            if categoryId == expected_categoryId:


                if sellerId not in sellers_gmv_dict['sellers']:
                    sellers_gmv_dict['sellers'][sellerId] = {"category":{}}
                    sellers_gmv_dict['sellers'][sellerId]["category"] = {categoryId:0}

                sellers_gmv_dict['sellers'][sellerId]['category'][categoryId] += price
    
    seller_gmv_list=[]

    #return output as list format
    for seller in sellers_gmv_dict['sellers']:
        seller_gmv = {"sellerID":seller,"categoryID":expected_categoryId,"GMV":sellers_gmv_dict['sellers'][seller]["category"][expected_categoryId]}
        seller_gmv_list.append(seller_gmv)


    return seller_gmv_list



def filter_out_key_sellers_and_order_by_gmv(all_sellers_gmv_list, all_sellers_list):
    #sort by gmv value decreasing
    newlist = sorted(all_sellers_gmv_list, key=lambda d: d['GMV']) 

    #set max output to 5
    if len(newlist) > 5:
        newlist = newlist[:5]


    for item in newlist:
        for seller_and_category in all_sellers_list:
            #check if sellers key category match and delete
            if item['sellerID'] == seller_and_category['sellerID'] and item['categoryID'] != seller_and_category['categoryID']:
                newlist.remove(item)

    seller_ids_ordered_by_gmv = [seller["sellerID"] for seller in newlist]

    return seller_ids_ordered_by_gmv



@app.route('/')
def home():
    return 'Hello, World!'

#endpoints to check all values from both sellers and orders
@app.route('/sellers')
def get_sellers():
    sellers = Sellers.query.all()
    sellers_list = [{'sellerID': s.sellerID, 'categoryID': s.categoryID} for s in sellers]
    return jsonify(sellers_list)


@app.route('/orders')
def get_orders():
    orders = Orders.query.all()

    orders_list = [{'orderID': o.orderID, 'itemIDs': o.itemIDs,
                    'categoryIDs': o.categoryIDs, 'sellerIDs': o.sellerIDs,
                    'orderstatus': o.orderstatus, 'price': o.price} for o in orders]
    return jsonify(orders_list)





@app.route('/top_sellers')
def top_sellers():
    
    all_sellers, all_category_ids = fetch_all_sellers_from_db()
    category = str(request.args.get('cat'))
    orders=fetch_orders_with_given_category_from_db(category)
    all_sellers_gmv_list = calculate_each_sellers_gmv_with_category(orders,all_category_ids,category)
    final_list = filter_out_key_sellers_and_order_by_gmv(all_sellers_gmv_list,all_sellers)

    
    return str(final_list)


if __name__ == '__main__':

    with app.app_context():
    # if not os.path.exists('Sellers.db') or os.path.exists('Orders.db'):
    #     db.create_all()
    # else:
        db.drop_all()
        db.create_all()


        # Load data from a JSON file and add users to the database
        with open('sellers_dump.json') as sellers_file:
            for seller_line in sellers_file:
                seller_dict = json.loads(seller_line)
                seller = Sellers(sellerID=seller_dict['sellerID'],categoryID=seller_dict['categoryID'])
                db.session.add(seller)
            

        with open('orders_dump.json') as orders_file:
            for order_line in orders_file:
                order_dict = json.loads(order_line)
                order = Orders(orderID=order_dict['orderID'],itemIDs=str(order_dict['itemIDs'])
                            ,categoryIDs=str(order_dict['categoryIDs']),sellerIDs=str(order_dict['sellerIDs'])
                            ,orderstatus=order_dict['orderstatus'],price=str(order_dict['price']))
                db.session.add(order)
                
            db.session.commit()    
    app.run(debug=True)
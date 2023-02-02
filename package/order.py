import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        

        orders = self.data['orders'].copy()
        delivered = orders[orders["order_status"] == "delivered"].copy()

        delivered["order_approved_at"]=pd.to_datetime(delivered["order_approved_at"])
        delivered["order_purchase_timestamp"]=pd.to_datetime(delivered["order_purchase_timestamp"])
        delivered["order_delivered_carrier_date"]=pd.to_datetime(delivered["order_delivered_carrier_date"])
        delivered["order_delivered_customer_date"]=pd.to_datetime(delivered["order_delivered_customer_date"])
        delivered["order_estimated_delivery_date"]=pd.to_datetime(delivered["order_estimated_delivery_date"])

        delivered["wait_time"] = delivered["order_delivered_customer_date"]-delivered["order_purchase_timestamp"]
        delivered['wait_time'] = delivered['wait_time'] / pd.to_timedelta(1, unit='D')

        delivered["expected_wait_time"] = delivered["order_estimated_delivery_date"]-delivered["order_purchase_timestamp"]
        delivered["expected_wait_time"] = delivered["expected_wait_time"] / pd.to_timedelta(1, unit='D')

        
        delivered["delay_vs_expected"]=0
       
        for index, row in delivered.iterrows():
            if delivered.loc[index,"expected_wait_time"]-delivered.loc[index,"wait_time"]<0:
                delivered.loc[index,"delay_vs_expected"] =delivered.loc[index,"expected_wait_time"]-delivered.loc[index,"wait_time"]
            else:
                delivered.loc[index,"delay_vs_expected"] = 0
        
        response_wait_time = delivered [["order_id", "wait_time", "expected_wait_time", "delay_vs_expected", "order_status"]]
        

        return response_wait_time

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews'].copy()
        
        reviews["dim_is_five_star"]=0
        reviews["dim_is_one_star"]=0    

        for index,row in reviews.iterrows():
            if reviews.loc[index,"review_score"]==5:
                reviews.loc[index,"dim_is_five_star"] = 1
            else:
                reviews.loc[index,"dim_is_five_star"] = 0

    

        for index,row in reviews.iterrows():
            if reviews.loc[index,"review_score"]==1:
                reviews.loc[index,"dim_is_one_star"] = 1
            else:
                reviews.loc[index,"dim_is_one_star"] = 0

        response_get_review_score = reviews[["order_id", "dim_is_five_star", "dim_is_one_star", "review_score"]]
        
        return response_get_review_score

    def get_number_products(self):
        products = self.data['order_items'].groupby(by="order_id",as_index=False).nunique()[["order_id","order_item_id"]]

        products = products.rename(columns = {"order_item_id": "number_of_products"})

        return products

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        sellers = self.data["order_items"].groupby(by="order_id",as_index=False)["seller_id"].nunique()
        sellers = sellers.rename(columns = {"seller_id": "number_of_sellers"})
        return sellers

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        price_and_freight = self.data["order_items"][["order_id","price","freight_value"]].groupby(by="order_id",as_index=False).sum()
        return price_and_freight    

 
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

            
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix',
                            as_index=False).first()

            
        sellers_mask_columns = [
                'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
            ]

        sellers_geo = sellers.merge(
        geo,
        how='left',
        left_on='seller_zip_code_prefix',
        right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
        geo,
        how='left',
        left_on='customer_zip_code_prefix',
        right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        customers_sellers = customers.merge(orders, on='customer_id')\
                .merge(order_items, on='order_id')\
                .merge(sellers, on='seller_id')\
                [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]
            
          
        matching_geo = customers_sellers.merge(sellers_geo,
                                                on='seller_id')\
                .merge(customers_geo,
                    on='customer_id',
                    suffixes=('_seller',
                                '_customer'))
           
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
        matching_geo.apply(lambda row:
                                haversine_distance(row['geolocation_lng_seller'],
                                                    row['geolocation_lat_seller'],
                                                    row['geolocation_lng_customer'],
                                                    row['geolocation_lat_customer']),
                                axis=1)
          
        order_distance =\
        matching_geo.groupby('order_id',
                                    as_index=False).agg({'distance_seller_customer':
                                                        'mean'})

        return order_distance

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
     
        training_set =\
            self.get_wait_time(is_delivered)\
                .merge(
                self.get_review_score(), on='order_id'
            ).merge(
                self.get_number_products(), on='order_id'
            ).merge(
                self.get_number_sellers(), on='order_id'
            ).merge(
                self.get_price_and_freight(), on='order_id'
            )
      
        if with_distance_seller_customer:
            training_set = training_set.merge(
                self.get_distance_seller_customer(), on='order_id')

        return training_set.dropna()
       
        

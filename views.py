from django.shortcuts import render
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
from core.models import Item
import numpy as np
# Create your views here.

conn = connect(host="tcp.cheerful-maggot.dataos.app",
               port="7432",
               auth=BasicAuthentication(
                   'balaji', "dG9rZW5fc2NhcmNlbHlfc2VyaW91c2x5X2ZyZXNoX2tpZC43YmJmMDIwZS0xMmJhLTRkNjEtYmFmZS0zNGQzNTcxZDZkOGQ="),
               http_scheme="https",
               http_headers={"cluster-name": "minervac"}  # eg:minervaa
               )


def create_prods(request):

    skus_df = pd.read_csv('./dbmigrate/skus/skus.csv')
    qr = "SELECT * FROM redshift.retail_accelerator.product WHERE sku_id IN ({0})".format(','.join(["'"+s+"'" for s in skus_df['sku_id']]))

    cat_qr = 'SELECT * FROM redshift.retail_accelerator.product_category'

    cat_df = pd.read_sql(cat_qr, conn)

    df = pd.read_sql(qr, conn)

    rename_dict = {'sku_id': 'id',
                   'product_name': 'title',
                   'list_price': 'price',
                   'sale_price': 'discount_price',
                   'product_category_id': 'category',
                   'product_subcategory_id': 'subcat_id',
                   'product_subcategory': 'subcat',
                   'product_description': 'description'
                   }

    df_out = df.rename(columns=rename_dict)
    df_out['slug'] = df_out['id'].copy()
    #df_out['discount_price'] = df_out['price'].apply(lambda x: float("{:.2f}".format(x*np.random.uniform(low = 0.6, high = 0.95))))

    df_out = df_out[list(rename_dict.values())+['slug']]

    df_out = df_out.merge(cat_df[['product_category', 'product_category_id']], left_on='category',
                          right_on='product_category_id', how='left')


    df_out.rename(columns={'product_category': 'image'}, inplace=True)
    df_out.drop(['product_category_id'], axis=1, inplace=True)

    df_out['image'] = df_out['image'].str.replace('mens', 'men')
    df_out['image'] = df_out['image'].str.replace('Mens', 'Men')

    df_out['image'] = df_out['image'].apply(lambda x: x+'.jpeg')
    df_out['label'] = ['P']*len(df_out)

    out_records = df_out.to_dict(orient='records')
    for r in out_records:
        Item.objects.create(id=r['id'], title=r['title'], price=r['price'],
                            discount_price=r['discount_price'], category=r['category'], slug=r['slug'], subcat_id = r['subcat_id'], subcat = r['subcat'], description=r['description'], image=r['image'], label=r['label'])

    return render(request, 'db_check.html')

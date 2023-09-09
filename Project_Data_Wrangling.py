#!/usr/bin/env python
# coding: utf-8

# In[1]:


#1. DATA CLEANSING
#import library
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
from matplotlib import pyplot as plt


# In[2]:


#import warnings
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)


# In[3]:


#Melihat isi database olist
conn = sqlite3.connect('olist.db')
list_table = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
print(list_table)


# In[4]:


#Membaca dan mengambil data dari database
conn = sqlite3.connect('olist.db')
query1 = 'SELECT * FROM olist_order_customer_dataset' 
customers = pd.read_sql(query1, conn)

query2 = 'SELECT * FROM olist_order_dataset' 
orders = pd.read_sql(query2, conn)

query3 = 'SELECT * FROM olist_order_items_dataset' 
order_items = pd.read_sql(query3, conn)

query4 = 'SELECT * FROM olist_order_payments_dataset' 
payments = pd.read_sql(query4, conn)

query5 = 'SELECT * FROM olist_products_dataset' 
products = pd.read_sql(query5, conn)

query6 = 'SELECT * FROM product_category_name_translation'
product_category_name_translation = pd.read_sql(query6, conn)


# In[5]:


#cek tipe data kolom tabel orders
orders.info()


# In[78]:


#parsing date
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])
orders['order_delivered_carrier_date'] = pd.to_datetime(orders['order_delivered_carrier_date'])
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])
orders['order_purchase_hour'] = pd.to_datetime(orders['order_purchase_timestamp']).dt.strftime('%H')


# In[7]:


#cek tipe data kolom tabel orders
orders.info()


# In[8]:


# Identifikasi outlier melalui statistik deskriptif
order_items.describe()


# In[9]:


#identifikasi outlier pada kolom price dan freight_value

fig, ax = plt.subplots(ncols=2, nrows=1,figsize = (15,6))

sns.histplot(data=order_items, x='price', ax= ax[0])
plt.ylabel('jumlah')

sns.histplot(data=order_items, x='freight_value', ax= ax[1])
plt.ylabel('jumlah')


# In[10]:


#handling outlier price

##Menghitung Q1 dan Q3 kolom price
Q1_price = order_items.price.quantile(0.25)
Q3_price = order_items.price.quantile(0.75)

##Menghitung batas bawah dan batas atas kolom price
batas_bawah_price = Q1_price - (Q3_price-Q1_price)*1.5
batas_atas_price = Q3_price + (Q3_price-Q1_price)*1.5

order_items_handling_outlier_price = order_items

##Menghitung Median dari kolom price
median_price = order_items['price'].median()

##melakukan imputasi data outlier dengan median
order_items_handling_outlier_price.loc[order_items_handling_outlier_price['price'] > batas_atas_price, 'price'] = median_price



#handling outlier freight_value

##Menghitung Q1 dan Q3 kolom freight_value
Q1_freight_value = order_items.freight_value.quantile(0.25)
Q3_freight_value = order_items.freight_value.quantile(0.75)

##Menghitung batas bawah dan batas atas kolom freight_value
batas_bawah_freight_value = Q1_freight_value - (Q3_freight_value-Q1_freight_value)*1.5
batas_atas_freight_value = Q3_freight_value + (Q3_freight_value-Q1_freight_value)*1.5

##Menghitung Median dari kolom freight_value
median_freight_value = order_items['freight_value'].median()

order_items_handling_outlier_price_n_freight_value = order_items_handling_outlier_price

##melakukan imputasi data outlier dengan median
order_items_handling_outlier_price_n_freight_value.loc[order_items_handling_outlier_price_n_freight_value['freight_value'] > batas_atas_freight_value, 'freight_value'] = median_freight_value


# In[11]:


#menampilkan distribusi tabel order items setelah handle outlier
fig, ax = plt.subplots(ncols=2, nrows=1,figsize = (12,8))

sns.histplot(data=order_items_handling_outlier_price_n_freight_value, x='price', ax= ax[0])

sns.histplot(data=order_items_handling_outlier_price_n_freight_value, x='freight_value', ax= ax[1])


# In[12]:


#identifikasi outlier kolom payment_value

fig, ax = plt.subplots(figsize= (10,6))

sns.histplot(data=payments, x='payment_value')


# In[13]:


# perhitungan Q1 dan Q3 data payment_value
Q1_payment_value = payments.payment_value.quantile(0.25)
Q3_payment_value = payments.payment_value.quantile(0.75)


# perhitungan batas bawah dan batas atas
batas_bawah_payment_value = Q1_payment_value - (Q3_payment_value - Q1_payment_value)*1.5
batas_atas_payment_value = Q3_payment_value + (Q3_payment_value - Q1_payment_value)*1.5

#perhitungan median kolom payment_value
median_payment_value = payments['payment_value'].median()

#imputasi median ke data outlier
payment_handling_outlier = payments
payment_handling_outlier.loc[payment_handling_outlier['payment_value'] > batas_atas_payment_value, 'payment_value'] = median_payment_value


# In[14]:


#menampilkan distribusi tabel payment setelah handle outlier
fig, ax = plt.subplots(figsize= (10,6))

sns.histplot(data=payment_handling_outlier, x='payment_value')


# In[15]:


#identifikasi missing value
(products.isna().sum()/len(products)*100).sort_values(ascending=False)


# In[16]:


#menampilkan tabel yang terdapat missing values
products_missing = products[products.isnull().any(axis=1)]
products_missing.head(10)


# In[17]:


#drop data yang terdapat missing values
products_handle_missing_values = products.dropna()


# In[18]:


#identifikasi missing value
(products_handle_missing_values.isna().sum()/len(products)*100).sort_values(ascending=False)


# In[26]:


#2. DATA ANALISIS
#a.Produk Yang Paling banyak diminati

#Menggabungkan kolom orders, payments dan order items
merge_order_orderItems = pd.merge(orders,order_items, on= 'order_id')
merge_order_orderItems_payments = pd.merge(merge_order_orderItems, payments, on= 'order_id')

#memilih kolom kolom yang ditampilkan
merge_order_orderItems_payments = merge_order_orderItems_payments[['order_id','order_status','order_purchase_timestamp','price','freight_value','payment_type','payment_value']]
merge_order_orderItems_payments.head(5)

#menggabungkan kolom orders, order_items, products dan product category name translation
merge_order_orderItems_products = pd.merge(merge_order_orderItems,products, on= 'product_id')
merge_order_orderItems_products_productCategory = pd.merge(merge_order_orderItems_products, product_category_name_translation, on= 'product_category_name', suffixes=('_order_items', '_product_category' ))

#menampilkan kolom yang dipilih
merge_order_orderItems_products_productCategory = merge_order_orderItems_products_productCategory[['order_id','price','product_category_name_english']]
merge_order_orderItems_products_productCategory


# In[27]:


# membuat grouping total penjualan berdasarkan kategori
grouping_penjualan_berdasarkan_kategori = merge_order_orderItems_products_productCategory.groupby('product_category_name_english').agg({'price':'sum'}).reset_index()
top5_penjualan_berdasarkan_kategori = grouping_penjualan_berdasarkan_kategori.nlargest(5,'price')
top5_penjualan_berdasarkan_kategori


# In[28]:


# membuat visualisasi

## membuat figure
plt.figure(figsize=(12, 6))

## membuat plot
sns.barplot(data=top5_penjualan_berdasarkan_kategori, x='product_category_name_english', y='price')

## membuat nama judul plot
plt.title('Total Penjualan Berdasarkan Kategori Produk', fontsize= 15)

## membuat nama label x
plt.xlabel('Kategori produk')

## membuat nama label y
plt.ylabel('Total Penjualan')

## menampilkan plot
plt.show()


# In[29]:


#b.Trend Penjualan dari bulan ke bulan

#menambahkan kolom month
merge_order_orderItems_payments['month'] = merge_order_orderItems_payments['order_purchase_timestamp'].dt.to_period('M')

#grouping
total_penjualan = merge_order_orderItems_payments.groupby('month')['payment_value'].sum()
df_total_penjualan = total_penjualan.reset_index()
df_total_penjualan.columns = ['Bulan', 'Total_Penjualan']
df_total_penjualan['Bulan'] = df_total_penjualan['Bulan'].dt.to_timestamp()
df_total_penjualan


# In[30]:


# visualisasi

## membuat grid
sns.set_style("whitegrid")

## membuat figure
plt.figure(figsize=(12, 6))

## membuat plot
sns.lineplot(data=df_total_penjualan, x='Bulan', y='Total_Penjualan')

## memberi judul plot
plt.title('Tren Penjualan dari Bulan ke Bulan', fontsize= 15)

## memberi nama label x
plt.xlabel('Bulan')

## memberi nama label y
plt.ylabel('Total Penjualan')

## menampilkan kolom y dengan angka real
plt.ticklabel_format(style='plain', axis='y')

## memiringkan label x
plt.xticks(rotation=45)

## menampilkan shop
plt.show()


# In[32]:


#c.5 Kota dengan pemesanan terbanyak
#menggabungkan tabel customers dan orders
merged_customers_orders = pd.merge(orders, customers, on='customer_id')


# In[33]:


#membuat tabel 5 kota dengan pemesanan terbanyak
top5_largest_orders = merged_customers_orders.groupby('customer_city').agg({'order_id': 'count'}).nlargest(5, 'order_id').reset_index()
top5_largest_orders


# In[34]:


#membuat visualisasi 

## membuat figure
fig, ax = plt.subplots(figsize = (10,6))

## membuat plot
sns.barplot(data=top5_largest_orders, x='customer_city', y='order_id')

## membuat nama judul plot
plt.title('5 kota dengan pemesanan terbanyak', fontsize= 15)

## membuat nama label x
plt.xlabel('kota')

## membuat nama label y
plt.ylabel('Jumlah Order')

## menampilkan plot
plt.show()


# In[36]:


#d.Metode Pembayaran yang paling banyak digunakan
# grouping tabel payments
total_penjualan_berdasarkan_metode_pembayaran = payments.groupby('payment_type').agg({'order_id':'count'}).reset_index().sort_values(by= 'order_id', ascending=False)
total_penjualan_berdasarkan_metode_pembayaran


# In[37]:


# membuat visualisasi

## membuat figure
plt.figure(figsize=(12, 6))

## membuat plot
sns.barplot(data=total_penjualan_berdasarkan_metode_pembayaran, x='payment_type', y='order_id')

## membuat nama judul plot
plt.title('Jumlah Pemesanan Berdasarkan Tipe Pembayaran', fontsize= 15)

## membuat nama label x
plt.xlabel('Tipe Pembayaran')

## membuat nama label y
plt.ylabel('Jumlah Order')

## menampilkan plot
plt.show()


# In[59]:


def count_plot(x, df, title, xlabel, ylabel, width, height, order = None, rotation=False, palette='winter', hue=None):
    ncount = len(df)
    plt.figure(figsize=(width,height))
    ax = sns.countplot(x = x, palette=palette, order = order, hue=hue)
    plt.title(title, fontsize=20)
    if rotation:
        plt.xticks(rotation = 'vertical')
    plt.xlabel(xlabel, fontsize=15)
    plt.ylabel(ylabel, fontsize=15)

    ax.yaxis.set_label_position('left')
    for p in ax.patches:
        x=p.get_bbox().get_points()[:,0]
        y=p.get_bbox().get_points()[1,1]
        ax.annotate('{:.1f}%'.format(100.*y/ncount), (x.mean(), y), 
                ha='center', va='bottom') # set the alignment of the text

    plt.show()
    
def bar_plot(x, y, df, title, xlabel, ylabel, width, height, order = None, rotation=False, palette='winter', hue=None):
    ncount = len(df)
    plt.figure(figsize=(width,height))
    ax = sns.barplot(x = x, y=y, palette=palette, order = order, hue=hue)
    plt.title(title, fontsize=20)
    if rotation:
        plt.xticks(rotation = 'vertical')
    plt.xlabel(xlabel, fontsize=15)
    plt.ylabel(ylabel, fontsize=15)

    ax.yaxis.set_label_position('left')
    for p in ax.patches:
        x=p.get_bbox().get_points()[:,0]
        y=p.get_bbox().get_points()[1,1]
        ax.annotate('{:.1f}%'.format(100.*y/ncount), (x.mean(), y), 
                ha='center', va='bottom') # set the alignment of the text

    plt.show()


# In[58]:


orders.order_status.value_counts()


# In[79]:


orders['order_purchase_hour'].value_counts()


# In[83]:


x = orders['order_purchase_hour']
order = [str(i).zfill(2) for i in range(24)]
count_plot(x, orders, 'Order Purchase per hour', 'Hour', 'num of orders', 12, 8, order=order, palette='deep')


# In[84]:


time_of_day = []
for time in orders['order_purchase_hour']:
    
    try:
        time = int(time)
        if time >= 6 and time < 12:
            time_of_day.append('Morning')
        elif time >= 12 and time < 17:
            time_of_day.append('Afternoon')
        elif time >= 17 and time <= 20:
            time_of_day.append('Evening')
        else:
            time_of_day.append('Night')
            
    except:
        time_of_day.append('Unknown')


# In[88]:


orders['classification_time_purchase'] = time_of_day


# In[86]:


orders['classification_time_purchase'].value_counts()


# In[87]:


x = orders['classification_time_purchase']
order = x.value_counts().index
count_plot(x, orders,'Total orders by time of the day', 'Time of day' , 'Frequency', 13,9, order=order, palette='dark')


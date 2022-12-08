# Databricks notebook source
# MAGIC %md
# MAGIC # Lue Aw_orders_archive.csv dataframena
# MAGIC 
# MAGIC Lue Aw_orders_archive.csv dataframena. 
# MAGIC 
# MAGIC Varmista, että tietomuodot ovat tarkoituksenmukaisia. Erityisesti varmista, että 
# MAGIC 
# MAGIC Päivämääräsarakkeet (OrderDate, DueDate, ShipDate ja ModifiedDate) ovat päivämäärien tallennukseen tarkoitettua datatyyppiä, ja 
# MAGIC 
# MAGIC Rahasummasarakkeet (SubTotal, TaxAmt, Freight ja TotalDue) ovat numeerista datatyyppiä 

# COMMAND ----------

# Task number one 1 made with UI

# File location and type
file_location = "/FileStore/harjoitusdata/Aw_orders_archive.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
#[Luke] Huomaa, että tämän jäljiltä numeeriset sarakkeet ovat merkkijonotyyppisiä. Raakadata on tuotu järjestelmästä, jossa desimaalierotin on pilkku (kuten varsin useassa ei-englanninkielisessä maassa). Tämä pitää käsitellä tuontivaiheessa. InferSchema toimii tiettyyn rajaan asti, mutta se voi olla hidas. Turvallisempaa on määritellä skeema käsin, jolloin myöskään ei ole vaaraa epädeterministisistä ilmiöistä eri lukukertojen välillä.
df_orders = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_orders)

# COMMAND ----------

#Task 1 made without UI

orders_csv_path ="dbfs:/FileStore/harjoitusdata/Aw_orders_archive.csv"

orders_df = (spark
           .read
           .option("sep", "\t") #[Luke] Huomaa sarake-erotin! Tässä puolipiste (ei tabulaattori), mikä näkyy tuossa skeemassa niin, että dataframessa on nyt vain yksi sarake.
           .option("header", True)
           .option("inferSchema", True)
           .csv(orders_csv_path)
          )

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lue Aw_customers.csv dataframena. 

# COMMAND ----------

# Task 2 made with UI

# File location and type
file_location = "/FileStore/harjoitusdata/Aw_customers.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ";"

# The applied options are for CSV files. For other file types, these will be ignored.
#[Luke] Huomaa myös tiedoston merkistökoodaus: lähdetiedosto on ANSIa, joten nyt erikoismerkit menevät väärin, kts. esim. asiakas 29497, Ferrier François
df_customers = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_customers)

# COMMAND ----------

#Task 2 made without UI

customers_csv_path ="dbfs:/FileStore/harjoitusdata/Aw_customers.csv"

customers_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(customers_csv_path)
          )

customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Uniikit id:t
# MAGIC 
# MAGIC Varmista, että aw_customers.csv:n asiakastiedot ovat todella uniikkeja (customerId:n perusteella)

# COMMAND ----------

# Here distinct, but it does not end in error if there is duplicates

distinct_customers = df_customers.distinct()
display(distinct_customers)

# COMMAND ----------

display(df_customers)

if df_customers.select("CustomerId").distinct() != df_customers.select("CustomerId").count():
    raise ValueError('Duplicates Detected in Customer Id')

#[Luke] Epäyhtälön vasemmalta puolelta puuttuu .count(). .distinct palauttaa dataframen, jota ei voi mielekkäästi verrata oikealla puolella olevaan kokonaislukuun.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Yhtenäistä puhelinnumerot
# MAGIC 
# MAGIC Yhtenäistä puhelinnumerot muotoon +1XXXXXXXXXXX ilman muita välimerkkejä tai välejä. Olkoot kaikki puhelinnumerot amerikkalaisia, joten lisää xxx-xxx-xxxx-muotoisten puhelinnumeroiden alkuun +1. Jätä tietueet, joiden muuttaminen ei onnistu, tyhjiksi arvoiksi (null). 

# COMMAND ----------

#How to get the +1? With Elif?
#[Luke] Esim. If-lauseen sisältävällä UDF:llä
from pyspark.sql import functions as unify_phonenumbers

df_customers = df_customers.withColumn("PhoneNumber", unify_phonenumbers.regexp_replace(unify_phonenumbers.regexp_replace(unify_phonenumbers.regexp_replace(unify_phonenumbers.regexp_replace(unify_phonenumbers.regexp_replace("PhoneNumber", "-", ""), "\\(", ""), "\\)", ""), " ", ""), "111", ""))

display(df_customers)


# COMMAND ----------

# MAGIC %md
# MAGIC # Asiakastiedot Delta-mutoiseksi tauluksi
# MAGIC 
# MAGIC Tallenna asiakastiedot Delta-muotoiseksi tauluksi nimeltä customers 

# COMMAND ----------

# Load the data from its source
# [Luke] Idea oli viedä nuo siistityt asiakastiedot tauluksi, mutta näinhän tämä menee
file_location ="dbfs:/FileStore/harjoitusdata/Aw_customers.csv"
file_type="csv"

infer_schema = "false"
first_row_is_header = "true"
delimiter = ";"

df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)


# Write the data to a table.
table_name = "Customers"
df.write.saveAsTable(table_name)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Vie tämän ja edellisvuoden tilaustiedot Delta-muotoiseksi tauluksi
# MAGIC 
# MAGIC Vie tämän ja edellisen kalenterivuoden tilaustiedot (OrderDaten perusteella) Delta-muotoiseksi tauluksi nimeltä orders_active. Älä vie kaikkia sarakkeita, vaan seuraavat: 
# MAGIC  - SalesOrderID 
# MAGIC  - OrderDate 
# MAGIC  - DueDate 
# MAGIC  - ShipDate 
# MAGIC  - SalesOrderNumber 
# MAGIC  - TerritoryId 
# MAGIC  - SubTotal 
# MAGIC  - TaxAmt 
# MAGIC  - Freight 
# MAGIC  - TotalDue 
# MAGIC  - asiakastiedoista Name nimellä CustomerName 
# MAGIC  - asiakastiedoista EmailPromotion 
# MAGIC  - asiakastiedoista PhoneNumber 
# MAGIC 
# MAGIC Viimeiset kolme saraketta tulee hakea asiakastietotaulusta kullekin tilausriville CustomerId:n perusteella. 
# MAGIC Yritä tehdä vienti turvautumatta rajapäivämäärän kovakoodaukseen, jotta koodi toimii muinakin vuosina oikein! 

# COMMAND ----------

# Load the data from its source.
file_location ="dbfs:/FileStore/harjoitusdata/Aw_orders_archive.csv", "dbfs:/FileStore/harjoitusdata/Aw_orders_20220630.csv"    #Jotta tämä toimisi, ympärillä pitäisi olla kulmasulkeet, jotta se olisi lista. Nyt se on tuple, joka ei kelpaa Sparkin dataframereaderille. Toisaalta tässä oli ajatuksena, että data liitettäisiin siihen jo aiemmin luotuun Delta-tauluun.
file_type="csv"

infer_schema = "false"  #nyt kaikesta tulee merkkijonotyyppistä, koska skeemaa ei ole määritelty
first_row_is_header = "true"
delimiter = ";"

df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)


# Write the data to a table.
table_name = "orders_active"
df.write.saveAsTable(table_name)

# Select only 
# df_orders_active = orders_active.select("SalesOrderID", "OrderDate", "DueDate", "ShipDate", "SalesOrderNumber", "TerritoryId", "SubTotal", "TaxAmt", "Freight", "TotalDue")
display(df_orders_active)

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregoi aikaisempaa tehtävää vanhemmat myyntitiedot arkistotauluksi
# MAGIC 
# MAGIC Aggregoi kohdan 7 myynitietoja vanhemmat myyntitiedot arkistotauluksi, joka sisältää sarakkeen TerritoryId, päivämääräsarakkeen ja sarakkeet SubTotal, TaxAmt, Freight ja TotalDue kuukausittain summattuina. Tallenna se nimellä orders_old. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lisää uusimmat tilaustiedot
# MAGIC 
# MAGIC Uusimmat tilaustiedot ovat Aw_orders_20220630.csv. Lue ne sisään vastaavalla tavalla kuin aiemmat tilaustiedot niin, että formaatti on yhteneväinen orders_active taulun tietojen kanssa. Lisää tiedot orders_active-tauluun. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Päivitä tietoja
# MAGIC 
# MAGIC Tiedostossa Aw_customers_updates on asiakastietojen päivityksiä. Päivitystiedoissa CustomerID toimii asiakasta määrittävänä avaimena. Päivitä sen perusteella customers-taulun asiakkaat seuraavasti: 
# MAGIC  - jos asiakas on jo taulussa, päivitä asiakkaan tiedot vastaamaan päivitetyn rivin tietoja 
# MAGIC  - jos asiakasta ei ole taulussa, lisää tiedot tauluun 
# MAGIC 
# MAGIC Vanhojen asiakkaiden EmailPromotion-arvo ei saa muuttua operaatiossa. Uusille asiakkailla EmailPromotion on oletusarvoisesti 0. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Arvokkaimpien tilausten tekijät
# MAGIC 
# MAGIC Etsi orders_active-taulusta ne viisi asiakasta, joiden keskimääräisen tilauksen arvo on ollut suurin tarkasteluaikana. 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Kehitys kuukausittain 
# MAGIC 
# MAGIC Piirrä summatun TotalDue-arvon kehitys kuukausittain orders_active-taulun tietojen keräysaikana. 

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Databricks-koulutus – harjoitustehtävä 
# MAGIC 
# MAGIC ### 1. Vie harjoitustehtävän datatiedostot (4 kpl) DBFS-kansiooni 
# MAGIC 
# MAGIC ### 2. Lue Aw_orders_archive.csv dataframena. 
# MAGIC    - Varmista, että tietomuodot ovat tarkoituksenmukaisia. Erityisesti varmista, että 
# MAGIC      - Päivämääräsarakkeet (OrderDate, DueDate, ShipDate ja ModifiedDate) ovat päivämäärien tallennukseen tarkoitettua datatyyppiä, ja 
# MAGIC      - Rahasummasarakkeet (SubTotal, TaxAmt, Freight ja TotalDue) ovat numeerista datatyyppiä 
# MAGIC       
# MAGIC ### 3. Lue Aw_customers.csv dataframena. 
# MAGIC 
# MAGIC ### 4. Varmista, että Aw_customers.csv:n asiakastiedot ovat todella uniikkeja (CustomerId:n perusteella). Pysäytä työkirjan ajo virheeseen, mikäli ne eivät ole.
# MAGIC 
# MAGIC ### 5. Yhtenäistä puhelinnumerot muotoon +1XXXXXXXXXXX ilman muita välimerkkejä tai välejä. Olkoot kaikki puhelinnumerot amerikkalaisia, joten lisää xxx-xxx-xxxx-muotoisten puhelinnumeroiden alkuun +1. Jätä tietueet, joiden muuttaminen ei onnistu, tyhjiksi arvoiksi (null).
# MAGIC 
# MAGIC ### 6. Tallenna asiakastiedot Delta-muotoiseksi tauluksi nimeltä customers 
# MAGIC 
# MAGIC ### 7. Vie tämän ja edellisen kalenterivuoden tilaustiedot (OrderDaten perusteella) Delta-muotoiseksi tauluksi nimeltä orders_active. Älä vie kaikkia sarakkeita, vaan seuraavat: 
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
# MAGIC  - (asiakastiedoista Name nimellä CustomerName)
# MAGIC  - (asiakastiedoista EmailPromotion)
# MAGIC  - (asiakastiedoista PhoneNumber)
# MAGIC   Viimeiset kolme saraketta tulee hakea asiakastietotaulusta kullekin tilausriville CustomerId:n perusteella. 
# MAGIC   Yritä tehdä vienti turvautumatta rajapäivämäärän kovakoodaukseen, jotta koodi toimii muinakin vuosina oikein! 
# MAGIC     
# MAGIC ### 8. Aggregoi kohdan 7 myynitietoja vanhemmat myyntitiedot arkistotauluksi, joka sisältää sarakkeen TerritoryId, päivämääräsarakkeen ja sarakkeet SubTotal, TaxAmt, Freight ja TotalDue kuukausittain summattuina. Tallenna se nimellä orders_old. 
# MAGIC 
# MAGIC ### 9. Uusimmat tilaustiedot ovat Aw_orders_20220630.csv. Lue ne sisään vastaavalla tavalla kuin aiemmat tilaustiedot niin, että formaatti on yhteneväinen orders_active taulun tietojen kanssa. Lisää tiedot orders_active-tauluun. 
# MAGIC 
# MAGIC ### 10. Tiedostossa Aw_customers_updates on asiakastietojen päivityksiä. Päivitystiedoissa CustomerID toimii asiakasta määrittävänä avaimena. Päivitä sen perusteella customers-taulun asiakkaat seuraavasti: 
# MAGIC   - jos asiakas on jo taulussa, päivitä asiakkaan tiedot vastaamaan päivitetyn rivin tietoja 
# MAGIC  - jos asiakasta ei ole taulussa, lisää tiedot tauluun 
# MAGIC     Vanhojen asiakkaiden EmailPromotion-arvo ei saa muuttua operaatiossa. Uusille asiakkailla EmailPromotion on oletusarvoisesti 0. 
# MAGIC     
# MAGIC ### 11. Etsi orders_active-taulusta ne viisi asiakasta, joiden keskimääräisen tilauksen arvo on ollut suurin tarkasteluaikana. 
# MAGIC 
# MAGIC ### 12. Piirrä summatun TotalDue-arvon kehitys kuukausittain orders_active-taulun tietojen keräysaikana. 
# MAGIC     Sparkin dokumentaatio on ystäväsi: Spark SQL and DataFrames - Spark 3.3.1 Documentation (apache.org) 

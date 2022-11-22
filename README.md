# pum_gcp
create table before run df2bq.py

- Create A BigQuery Dataset and Table
Just like the Cloud Storage bucket, creating a BigQuery dataset and table is very simple. Just remember that you first create a dataset, then create a table. 
When you create your BigQuery table, you’ll need to create a schema with the following fields. These BigQuery fields match the fields in the Thailand air quality json API’s header.
Create the BigQuery table, which should have a schema that looks like this.


 ![image](https://user-images.githubusercontent.com/93181638/203223627-ab54944d-5b71-4132-9094-81dab4c2360c.png)


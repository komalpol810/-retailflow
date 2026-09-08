ALTER TABLE gold.dim_product ADD CONSTRAINT dim_product_pkey PRIMARY KEY (product_key);
ALTER TABLE gold.dim_seller ADD CONSTRAINT dim_seller_pkey PRIMARY KEY (seller_key);
ALTER TABLE gold.dim_geography ADD CONSTRAINT dim_geography_pkey PRIMARY KEY (geography_key);
ALTER TABLE gold.dim_date ADD CONSTRAINT dim_date_pkey PRIMARY KEY (date_key);
ALTER TABLE gold.dim_order_status ADD CONSTRAINT dim_order_status_pkey PRIMARY KEY (order_status_key);

SOURCE_TABLES = {

    "inventory": {

        "file": "/Volumes/workspace/default/test/beverage_source_files/inventory.csv",

        "bronze": "bronze_beverages.inventory",

        "silver": "silver_beverages.inventory",

        "primary_key": "inventory_id"

    },

    "product": {

        "file": "/Volumes/workspace/default/test/beverage_source_files/product.csv",

        "bronze": "bronze_beverages.product",

        "silver": "silver_beverages.product",

        "primary_key": "product_id"

    },

    "retailer": {

        "file": "/Volumes/workspace/default/test/beverage_source_files/retailer.csv",

        "bronze": "bronze_beverages.retailer",

        "silver": "silver_beverages.retailer",

        "primary_key": "retailer_id"

    },

    "sales": {

        "file": "/Volumes/workspace/default/test/beverage_source_files/sales.csv",

        "bronze": "bronze_beverages.sales",

        "silver": "silver_beverages.sales",

        "primary_key": "transaction_id"

    },

    "store": {

        "file": "/Volumes/workspace/default/test/beverage_source_files/store.csv",

        "bronze": "bronze_beverages.store",

        "silver": "silver_beverages.store",

        "primary_key": "store_id"

    }

}
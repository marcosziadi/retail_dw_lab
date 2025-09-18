import pandas as pd

class DimBuilder:
    def __init__(self, config):
        self.config = config
    
    def build_dim_product(self, products_df, categories_df):
        """
        Creates product dimension by joining products and categoties tables
        """

        # Join on FK parent_category_id
        categories_with_parents_df = pd.merge(
            categories_df,
            categories_df[["category_id", "category_name"]].add_prefix("parent_"),
            on = "parent_category_id",
            how = "left",
        )[["category_id","category_name","parent_category_name"]]

        # LEFT JOIN
        dim_product = pd.merge(
            products_df,
            categories_with_parents_df,
            on = "category_id",
            how = "left"
        )

        # NEW SURROGATE KEY
        dim_product['product_key'] = range(1, len(dim_product) + 1)

        print(f"dim_product table was successfully created. Rows: {len(dim_product)}")
        return dim_product
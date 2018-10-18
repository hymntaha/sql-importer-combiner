import csv
import pymysql
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData,text

def db_connection():
    engine = create_engine("mysql://root@127.0.0.1/durkee_food_services")
    con = engine.connect()
    return engine

def insert_csv_to_sql():
    db_connection()
    connection = db_connection()
    csv_df = pd.read_excel('tblProductNutrition.xlsx')
    pd.set_option('display.max_colwidth', -1)
    csv_df.index = csv_df.index + 1
    # print(csv_df['unqProductDescriptionId'],csv_df['unqProductId'],csv_df['vchProductDescription'],csv_df['vchAdditionalDescription'],csv_df['vchShortDescription'],csv_df['vchServingSuggestions'],csv_df['vchPreparation'],csv_df['vchStorage'],csv_df['intShelfLifeDays'])
    # print(csv_df['vchPreparation'])
    # csv_df.to_sql(name='product_descriptions', con=con, if_exists='replace', index=False)

    with connection.connect() as conn, conn.begin():
        csv_df.to_sql('product_nutrition', conn, if_exists='replace')

    print('Thank you for your patience! Importer is completed')

def merge_two_tables_with_foreign_key():
    db_connection()
    connection = db_connection()
    # csv_df1 = pd.read_excel('tblProducts.xlsx')
    # csv_df2 = pd.read_excel('tblProductNutrition.xlsx')
    # pd.set_option('display.max_colwidth', -1)


    # df_merged = pd.merge(csv_df1,csv_df2, left_on=['unqProductId','vchProductName','bitConsumerUnit','datModified','datPublished','vchTargetMarket','vchReferenceId','vchBrandName'],
    #                      right_on=['unqProductNutritionId','vchNutrient	vchPrecision',	'decDailyValuePercent',	'decQuantity',	'vchQuantityUnit'], how='inner')
    # df_merged = pd.merge(csv_df1, csv_df2, on='unqProductId', how='left')
    sql = text('SELECT * FROM recipes LEFT JOIN recipe_brands ON recipes.unqRecipeId = recipe_brands.unqRecipeId UNION SELECT * FROM recipes RIGHT JOIN recipe_brands ON recipes.unqRecipeId = recipe_brands.unqRecipeId')

    result = connection.execute(sql)
    rows_list = []
    for row in result:
        dict1 = {}
        dict1.update(row)
        rows_list.append(dict1)

    df = pd.DataFrame(rows_list)
    pd.set_option('display.max_colwidth', -1)
    # df.drop(['level_0'], axis=1)

    # csv_df = pd.read_sql_query(result, con=connection.connect())
    #
    # print(csv_df)
    # key = 0
    # for row in result:
    #     key = key+1
    #     print(key)
        # print(row)
    # print(csv_df['unqProductDescriptionId'],csv_df['unqProductId'],csv_df['vchProductDescription'],csv_df['vchAdditionalDescription'],csv_df['vchShortDescription'],csv_df['vchServingSuggestions'],csv_df['vchPreparation'],csv_df['vchStorage'],csv_df['intShelfLifeDays'])
    # print(csv_df['vchPreparation'])
    # csv_df.to_sql(name='product_descriptions', con=con, if_exists='replace', index=False)

    with connection.connect() as conn, conn.begin():
        df.to_sql('recipes_test', conn, if_exists='replace')

    print('Thank you for your patience! Importer is completed')

# insert_csv_to_sql()
merge_two_tables_with_foreign_key()

# Merges:
# Product-claims
# Product-allergen statement
# Product-Category
# Product-Description
# Product-Identifier
# Product-Image
# Product-Ingredients
# Product-Measurement
# Product-serving_sizes
# Recipe- recipe brand

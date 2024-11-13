from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank, desc, lit, sum


def get_result(table_dict):
    menu = """
Queries:
        1 - Display the number of movies in each category, sort in descending order.

        2 - Display the top 10 actors whose movies were rented the most, sort in descending order.

        3 - Display the movie category that was spent the most money on.

        4 - Display the names of movies that are not in inventory.

        5 - Display the top 3 actors who appeared in the most movies in the “Children” category.
        If several actors have the same number of movies, display them all.

        6 - Display cities with the number of active and inactive customers (active — customer.active = 1)
        Sort by the number of inactive customers in descending order.

        7 - Display the movie category that has the largest number of total rental hours in cities
        (customer.address_id in this city), and that begin with the letter “a”.
        Do the same for cities that have the '-' symbol.

Choose a query or print 0 to exit\n"""
    while True:
        choose = input(menu)
        if choose == "0":
            break
        elif choose not in ("1", "2", "3", "4", "5", "6", "7"):
            print("Please input number from 0 to 7")
        else:
            run_query(table_dict, choose)


def run_query(table_dict, num):
    df_film = table_dict["film"]
    df_film_category = table_dict["film_category"]
    df_category = table_dict["category"]
    df_film_actor = table_dict["film_actor"]
    df_actor = table_dict["actor"]
    df_inventory = table_dict["inventory"]
    df_customer = table_dict["customer"]
    df_address = table_dict["address"]
    df_city = table_dict["city"]
    df_rental = table_dict["rental"]
    if num == "1":
        df_film.join(df_film_category, "film_id").groupby("category_id").count().join(
            df_category, "category_id"
        ).select(["name", "count"]).sort(desc("count")).collect()
    if num == "2":
        df_film.join(df_film_actor, "film_id").join(df_actor, "actor_id").groupby(
            ["first_name", "last_name"]
        ).agg(sum("rental_duration").alias("sum_ren_dur")).sort(desc("sum_ren_dur")).limit(
            10
        ).show()
    if num == "3":
        df_film.join(df_film_category, "film_id").join(df_category, "category_id").groupby(
            "name"
        ).agg(sum("replacement_cost").alias("sum_cost")).sort(desc("sum_cost")).limit(1).show()
    if num == "4":
        df_film.join(df_inventory, "film_id", "leftanti").select("title").sort("title").show()
    if num == "5":
        df_film.join(df_film_category, "film_id").join(
            df_category.where(df_category["name"] == "Children"), "category_id"
        ).join(df_film_actor, "film_id").join(df_actor, "actor_id").groupby(
            ["first_name", "last_name"]
        ).count().withColumn(
            "dns_rnk", dense_rank().over(Window.orderBy(desc("count")))
        ).where(
            col("dns_rnk") <= 3
        ).select(
            ["first_name", "last_name", "count"]
        ).show()
    if num == "6":
        df_customer.join(df_address, "address_id").join(df_city, "city_id").groupby(
            ["city", "active"]
        ).count().sort(desc("active"), desc("count")).show()
    if num == "7":
        df_temp = (
            df_film.join(df_film_category, "film_id")
            .join(df_inventory, "film_id")
            .join(df_rental, "inventory_id")
            .join(df_customer, "customer_id")
            .join(df_address, "address_id")
            .join(
                df_city.where(
                    col("city").like("a%") | col("city").like("A%") | col("city").like("%-%")
                ),
                "city_id",
            )
            .join(df_category, "category_id")
        )
        df_first = (
            df_temp.where(col("city").like("a%") | col("city").like("A%"))
            .groupby("name")
            .agg(sum("rental_duration").alias("max_rent"))
            .sort(desc("max_rent"))
            .limit(1)
            .withColumn("city_template", lit("a.. or A.."))
        )
        df_second = (
            df_temp.where(col("city").like("%-%"))
            .groupby("name")
            .agg(sum("rental_duration").alias("max_rent"))
            .sort(desc("max_rent"))
            .limit(1)
            .withColumn("city_template", lit("..-.."))
        )
        df_first.union(df_second).show()

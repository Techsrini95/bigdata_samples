# bigdata_samples
here all folders contains bigdata working exmaples

### Code Explanation:(Cust_order)
1. **Spark Configuration and Context:**
   - `SparkConf`: Configures Spark settings such as the application name, master URL, and other settings.
   - `SparkContext`: Initializes the Spark application using the configuration, which allows you to interact with Spark's distributed computation system.
   
2. **Spark Session:**
   - `SparkSession`: Required for Spark SQL operations. It allows access to DataFrame and SQL API (though not used here, it’s prepared).

3. **RDD Operations:**
   - `rdd.textFile`: Loads the dataset from a CSV file into an RDD (Resilient Distributed Dataset).
   - `.map(x => x.split(","))`: Splits each line of the CSV into an array of strings based on the comma delimiter.
   - `.map(arr => (arr(0), arr(2).toFloat))`: Creates a tuple where the first element is the customer ID and the second is the price of the order.
   - `reduceByKey(_ + _)`: Reduces the RDD by summing up the prices for each customer ID.

4. **Sorting:**
   - `.sortBy(x => math.ceil(x._2).toLong, ascending = false)`: Sorts the customers by the total order price in descending order (rounding the price value).

5. **Output:**
   - `take(5)`: Retrieves the top 5 customers based on the sorted order.
   - `foreach(println)`: Prints the top 5 customers to the console.


   Thankyou!


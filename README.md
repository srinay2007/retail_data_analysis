# retail_data_analysis
This is case study project with Kafka and Pyspark

Digitally enabled customers like to shop on the run, and that is the reason why online shopping is one of the most popular online activities worldwide. In 2019, global e-commerce sales amounted to 3.53 trillion USD and are projected to grow to 6.54 trillion USD in 2022.

The analytical capabilities of big data have had a positive impact across industries, including e-commerce. Big data tools improve business performance by enabling companies to analyse trends and current consumer behavioural patterns and offer better and more customised products.

For the purposes of this project, you have been tasked with computing various Key Performance Indicators (KPIs) for an e-commerce company, RetailCorp Inc. You have been provided real-time sales data of the company across the globe. The data contains information related to the invoices of orders placed by customers all around the world. You will get to know the details of the data in the next segment.

At the industry level, an end-to-end data pipeline is built for this purpose. Tools such as HDFS(Hadoop Distributed File System) are used to store the data that is processed by the real-time processing framework and then shown on a dashboard with tools such as Tableau and PowerBI. The image given below is an example of such a complete data pipeline.

![image](https://github.com/srinay2007/retail_data_analysis/assets/98680554/7d515251-ffbd-4c0b-afb0-2577b0cd3f1c)

                                            Data Pipeline in the Industry

![image](https://github.com/srinay2007/retail_data_analysis/assets/98680554/5b31f2c0-6959-43c3-8ca8-ebaf31711ac2)

                                            Architecture of the Project

you are expected to perform the following tasks in this project:

- Reading the sales data from the Kafka server
- Preprocessing the data to calculate additional derived columns such as total_cost etc
- Calculating the time-based KPIs and time and country-based KPIs
- Storing the KPIs (both time-based and time- and country-based) for a 10-minute interval into separate JSON files for further analysis

You will 4 KPIs on a tumbling window of one minute on orders across the globe.

Also you wll calculate first 4 KPIs on country basis.

1.**Total Volume of Sales** = ∑(quantity∗unitprice)
By calculating this, you can see where and when demand is higher and try to understand the reason. It helps in creating projections for the future. The total volume of sales is the sum of the transaction value of all orders in the given time window. The total transaction value of a specific order will be calculated as follows:
**∑(quantity∗unitprice)**
 
The total volume of sales in a time interval can be calculated as the summation of the transaction values of all the orders in that time interval. Also, the sales data stream contains a few returns. In the case of returns, the transaction amount needs to be subtracted. So, the equation for the total volume of sales, in this case, will be calculated as follows:
**∑Order(quantity∗unitprice)−∑Return(quantity∗unitprice)**

2.**OPM (orders per minute)**
Orders per minute (OPM) is another important metric for e-commerce companies. It is a direct indicator of the success of the company. As the name suggests, it is the total number of orders received in a minute.
**∑Invoices**

3.**Rate of return: **
No business likes to see a customer returning their items. Returns are costly because they need to be processed again and have adverse effects on revenue. The rate of return indicates customer satisfaction for the company. The more satisfied the customer is, the lower the rate of return will be. The rate of return is calculated against the total number of invoices using the following equation:
∑Returns   /  ( ∑ Returns+∑Orders)

4.**Average transaction size:**
The average transaction size helps in measuring the amount of money spent on average for each transaction. Evaluating this KPI over a year is a good indicator of when the customers are more likely to spend money, which enables the company to adapt their advertising accordingly. This can be calculated using the following equation:
**TotalSalesVolume/ (∑Returns+∑Orders)**
 You will be calculating the four KPIs mentioned above for all the orders. You will also be calculating the first three KPIs on a per-country basis.
Note that in some cases, the total sales volume can be negative if the return cost is more than that of new orders in that window.




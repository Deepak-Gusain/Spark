from pyspark.sql import SparkSession
from pyspark.sql.functions import  when , to_date , round
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName('covid19-mortality-rate-analysis').config('').getOrCreate()

#loading datasets
raw_data_df = spark.read.csv("D:/Training/kaggle_dataset_coronaviru_sample/novel-corona-virus-2019-dataset/covid_19_data_rought.csv" ,\
                        header = True , inferSchema = True )

#rename a few col
raw_data_df_tmp = raw_data_df.withColumnRenamed('Province/State','ProvinceOrState').withColumnRenamed('Country/Region','CountryOrRegion')

#cleaning and filter data
drop_cols = ['SNo','Recovered','Last Update']
filtered_data_df = raw_data_df_tmp.filter((raw_data_df_tmp.Confirmed > raw_data_df_tmp.Recovered)).drop(*drop_cols).fillna('unknown',subset=['ProvinceOrState'])

date_correction_df = filtered_data_df.select( when(to_date( F.col("ObservationDate") ,"MM/dd/yyyy").isNotNull() , \
                          to_date( F.col("ObservationDate") ,"MM/dd/yyyy")). \
                          when(to_date( F.col("ObservationDate") ,"MM-dd-yyyy").isNotNull() , \
                          to_date( F.col("ObservationDate") ,"MM-dd-yyyy")).otherwise("Unknown Format").alias("Date") , \
                          filtered_data_df['ProvinceOrState'] ,  filtered_data_df['CountryOrRegion'] , \
                          filtered_data_df['Confirmed'], filtered_data_df['Deaths'])

date_correction_df.createOrReplaceTempView('Covid19_dataset_tbl')

#get the latest entry of every state in a country and get sum of all the cases and deaths
groupby_states = """select CountryOrRegion , ProvinceOrState , Confirmed ,Deaths, latest_date 
                    from (select CountryOrRegion , ProvinceOrState , Confirmed , Deaths, Date , max(Date)
                    over( partition by ProvinceOrState) as Latest_date from Covid19_dataset_tbl ) 
                    as sub where Latest_date = Date """

latest_city_entry_df = spark.sql( groupby_states )
country_count_df_tmp = latest_city_entry_df.groupBy("CountryOrRegion").sum("Confirmed", "Deaths")
country_count_df = country_count_df_tmp.withColumnRenamed( 'sum(Confirmed)' , 'Total_Confirmed'). \
                   withColumnRenamed( 'sum(Deaths)' , 'Total_Deaths')

#cal mortality rate of 10 higest contries
country_mortality_rate_tmp_df = country_count_df.filter((country_count_df.Total_Confirmed  > 100)).withColumn('Rate' , \
                            round(((country_count_df['Total_Deaths'] / country_count_df['Total_Confirmed'])*100 ) , 2))

country_mortality_rate_df = country_mortality_rate_tmp_df.orderBy(country_mortality_rate_tmp_df.Rate.desc()).limit(10)

#converting spark dataframe into pandas dataframe and plot graph
panda_df = country_mortality_rate_df.toPandas()
country_covid_graph = sns.barplot(y = 'CountryOrRegion' , x = 'Rate' , data = panda_df , color = 'red' )
plt.title('Top 10 countries in COVID-19 mortality rate')
plt.show()

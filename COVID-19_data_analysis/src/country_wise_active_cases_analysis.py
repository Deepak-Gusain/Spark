from pyspark.sql import SparkSession
from pyspark.sql.functions import  when , to_date
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName('covid19-countrywise-analysis').config('').getOrCreate()

#loading datasets
raw_data_df = spark.read.csv("D:/Training/kaggle_dataset_coronaviru_sample/novel-corona-virus-2019-dataset/covid_19_data_rought.csv" ,\
                        header = True , inferSchema = True )

#rename few col
raw_data_df_tmp = raw_data_df.withColumnRenamed('Province/State','ProvinceOrState'). \
                  withColumnRenamed('Country/Region','CountryOrRegion')

# cal active cases and fill null value with 0
data_with_act_count_df_tmp = raw_data_df_tmp.withColumn('Active_Cases' , \
                             raw_data_df_tmp['Confirmed']-raw_data_df_tmp['Deaths']-raw_data_df_tmp['Recovered']). \
                             fillna(0 , subset=['Active_Cases'])

#filter and remove unnecessary entries
drop_columns = ['Last Update','Deaths','Recovered','SNo','Confirmed']
data_with_act_count_df_tmp = data_with_act_count_df_tmp.filter((data_with_act_count_df_tmp.Confirmed > \
                             data_with_act_count_df_tmp.Recovered)).drop(*drop_columns)

data_with_act_count_df = data_with_act_count_df_tmp.fillna('unknown' , subset=['ProvinceOrState'])

date_correction_df = data_with_act_count_df.select( when(to_date( F.col("ObservationDate") ,"MM/dd/yyyy").isNotNull() , \
                          to_date( F.col("ObservationDate") ,"MM/dd/yyyy")). \
                          when(to_date( F.col("ObservationDate") ,"MM-dd-yyyy").isNotNull() , \
                          to_date( F.col("ObservationDate") ,"MM-dd-yyyy")).otherwise("Unknown Format").alias("Date") , \
                          data_with_act_count_df['CountryOrRegion'] , data_with_act_count_df['Active_Cases'] , \
                          data_with_act_count_df['ProvinceOrState'])

date_correction_df.createOrReplaceTempView('covid_rec_tbl')

#get the latest entry of every state in a country
latest_entry_query = """select CountryOrRegion , ProvinceOrState , Active_Cases , latest_date 
                        from (select CountryOrRegion , ProvinceOrState , Active_Cases , Date , max(Date)
                        over( partition by ProvinceOrState) as Latest_date from covid_rec_tbl ) as sub 
                        where Latest_date = Date """

latest_city_entry_df = spark.sql(latest_entry_query)

#get the total count of active cases and get top countries
latest_city_entry_df.createOrReplaceTempView('covid_groupby_state_tbl')
total_act_cases_query = """select sum(Active_Cases) as Total_Active_Cases , CountryOrRegion 
                           from covid_groupby_state_tbl group by CountryOrRegion order by 
                           Total_Active_Cases desc limit 10"""

final_dataset = spark.sql( total_act_cases_query )

#converting spark dataframe into pandas dataframe and plot graph
panda_df = final_dataset.toPandas()
country_covid_graph = sns.barplot(y = 'CountryOrRegion' , x = 'Total_Active_Cases' , data = panda_df , color = 'Blue' )
plt.title('Top 10 countries in COVID-19 cases');
plt.show()

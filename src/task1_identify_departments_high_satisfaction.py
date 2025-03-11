# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments where more than 5% of employees have 
    SatisfactionRating > 4 and Engagement Level 'High'.
    """
    dept_total = df.groupBy("Department").agg(count("*").alias("TotalEmployees"))
    high_satisfaction = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))
    dept_high_satisfaction = high_satisfaction.groupBy("Department").agg(count("*").alias("HighSatEngaged"))
    
    result_df = dept_total.join(dept_high_satisfaction, "Department", "left") \
        .withColumn("Percentage", spark_round((col("HighSatEngaged") / col("TotalEmployees")) * 100, 2)) \
        .filter(col("Percentage") > 5) \
        .select("Department", "Percentage")
    
    return result_df

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()
    input_file = "input/employee_data.csv"
    output_file = "outputs/departments_high_satisfaction.csv"
    
    df = load_data(spark, input_file)
    result_df = identify_departments_high_satisfaction(df)
    write_output(result_df, output_file)
    
    spark.stop()

if __name__ == "__main__":
    main()
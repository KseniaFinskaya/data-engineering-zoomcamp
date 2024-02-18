import dlt
import duckdb

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

# Append generator to people table
generators_pipeline = dlt.pipeline(
						destination='duckdb', 
						dataset_name='generators')

# run the pipeline with generator 1 function
info = generators_pipeline.run(people_1(), 
                    table_name="people", 
                    write_disposition="replace")

# show the outcome generator 1
print(info)

# append generaotr 2 data
info = generators_pipeline.run(people_2(), 
                    table_name="people", 
                    write_disposition="append")
# show the outcome
print(info)

# connect to duckdb database
conn=duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.pipeline_name}'")
# answer age sum question
sum_ages = conn.sql("SELECT SUM(Age) FROM generators.people").df()
print(sum_ages)
# 353.0


# Merge a generator into new table people 2 by ID column
# run the pipeline with generator 1 function
info_2 = generators_pipeline.run(people_1(), 
                    table_name="people_2", 
                    write_disposition="replace")

# show the outcome generator 1
print(info_2)

# append generaotr 2 data
info_2 = generators_pipeline.run(people_2(), 
                    table_name="people_2", 
                    write_disposition="merge",
                    primary_key="ID")
# show the outcome
print(info_2)

# connect to duckdb database
conn=duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{generators_pipeline.pipeline_name}'")
# answer age sum question
sum_ages = conn.sql("SELECT SUM(Age) FROM generators.people_2").df()
print(sum_ages)
# 266.0
# **Overview**

The project is designed to support an end-to-end data quality validation workflow on a **MySQL-based** *`inventory`* database that serves as a source for DataHub ingestion. It combines containerized services (*using Docker Compose*) and a Python application that performs validations on data extracted from the database. The primary steps include:

- Setting up environment variables (*defined in `.env`*) and Docker containers via `docker-compose.yaml.`
- Preparing the MySQL `inventory` table using a dedicated script (*`_prepare_database.py`*).
- Running the main application (*`app/app.py`*) which utilizes `SQLAlchemy` to retrieve data in chunks and `pandas` for vectorized processing.
- Reporting data quality results as assertions to DataHub through the `DataHub API` ingestion.

# **Main Logic and Workflow**

- **Initialization:** The application loads configurations (from `.env` and `controller_configs.yaml`) and instantiates supporting components including the database handler, assertion handler, and controllers.
- **Data retrieval:** The Database class establishes a MySQL connection using SQLAlchemy and retrieves data in chunks for efficiency.
- **Data validation:** For each retrieval chunk, controllers (*like *`ProductController`*) perform multiple data quality checks such as null, negative, and invalid values.
- **Assertion Reporting:** After validations, results are collected and reported to DataHub via the `AssertionHandler` class.
- **Cleanup:** Resources and connections are closed properly after processing.

# **Vectorized and Batch Processing Mechanism**
### Batch Processing
The use of SQLAlchemy in combination with pandas’ chunked reading (*using pd.read_sql with chunksize*) allows the application to retrieve large datasets in manageable batches. This approach reduces memory usage compared to loading the entire dataset at once.

### Vectorized Operations
Pandas vectorized functions are utilized throughout the controllers. Operations such as counting null values, verifying column conditions (e.g., non-positive prices or overvalued stock), and applying conditional checks are executed using pandas built-in vectorized methods.

# **Design Choices**

### Factory Pattern 

- **For Database:** The `DatabaseFactory` and its implementation (*`MySQLDatabaseFactory`*) are responsible for creating SQLAlchemy engine instances and connections. 

- **For Controllers:** A `ControllerFactory` is employed to register and create controllers (*like `ProductController`*), allowing separation of data validation logic.

# **Components**

#### docker-compose.yaml
Constructed based on the official Compose YAML (*reference: [official file](https://raw.githubusercontent.com/datahub-project/datahub/master/docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml)*) and was modified to include an additional simple MySQL container as a source database:
  ```yaml
  source_mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: inventory
    ...
    volumes:
      - source_mysql:/var/lib/mysql
  ```
---

#### recipes/inventory.yml

Configuration for metadata ingestion into DataHub. It specifies the MySQL source and the DataHub REST sink.

---

#### app/config.py

Loads environment variables via python-dotenv and defines constants such as connection strings and chunk sizes. Custom logging configuration is established so that runtime behaviors and issues are captured and can be tracked through log outputs.

---

#### app/database.py
- Responsible for handling Database interactions.
- Uses the factory design pattern to establish database connections via SQLAlchemy.
- Provides methods for creating engine, connections and data retreival.

---

#### app/controllers/*.py
- Reponsible for handling data quality controls.
- `BaseController` is shared which offers common operations and `ProductController` is a custom controller inherit from `BaseController` for table `products`.
- Uses the factory pattern for dynamic registration and instantiation, enabling easy addition of new controllers with custom logic for different tables.

---

#### app/controller_configs.yaml
- Specifies controller settings such as controller name, table name, columns, and the necessary specifications for the controller that is desired to be created
- Allows seamless addition or modification of controllers by simply updating this YAML configuration file.

---

#### app/assertion_handler.py
- Handles communication with DataHub using the `DataHubGraph` client.
- Upserts and reports custom assertions for user-defined quality checks.

#### app/executor.py
- Manages the overall validation process by iterating through data chunks and invoking each controller’s validations.
- Aggregates results, measures execution time, and delivers reporting to the `AssertionHandler`.
- Decouples processing logic from quality validations, making it easier to extend and modify.

---

#### app/app.py

- Serves as the entry point of the application.
- Initializes the workflow by loading configuration and instantiating components such as the database, assertion handler, and controllers.
- Executors are dynamically created using registered controllers via the `ControllerFactory`.
- Orchestrates execution of executors.

# **Configurations**

### Source MySQL
The MySQL instance, which serves as the metadata source for the project, runs on **`localhost:3377`**. Its configuration is kept basic to maintain project simplicity, with the **username** set to **`root`** and the **password** to **`root`**. It can be queried using a client (*e.g., `DBeaver`*) once the above specifications are properly configured.

---

### Logging
A logging mechanism is established in the project. Processes can be tracked step-by-step through logs.

For example (for 1M records):

    2025-03-09 14:35:42 - [QUALITY_CHECKS] - INFO - Execution time: 3.25 seconds.

---

### _prepare_database.py

The script connects to a MySQL database, creates a products table if it does not exist, clears its contents, generates random product data, and inserts 1,000,000 records. 
name: The name column is generated using a simple formatted string. Example, `f'Product {i}'`

**id:** `AUTO_INCREMENT`

**category:** The category column is populated randomly from the combined list of valid_categories and invalid_categories. There is no weighting or probability involved.

**price:** The price column is created using random.choices(). 95% of the time, a price is selected as floating-point number between -50.0 and 500.0. This value is rounded to two decimal places. 5% of the time, None is selected, simulating missing data for the price.

**stock:** The stock column is also created using random.choices(). 95% of the time, stock value is selected as integer between -50 and 5000. 5% of the time, None is chosen, simulating missing data for the stock quantity.

# **Running & Observations**

## Prerequisites

- **Docker** must be installed on your computer.
- **Python** must be installed to create a virtual environment.

## Running the Application

### 1. Running `setup.sh`

A shell script is prepared to kick off the project easily. It performs the following steps:

1. Create and activate a virtual environment.
2. Upgrade essential Python tools (`pip`, `wheel`, `setuptools`).
3. Install project dependencies from `requirements.txt`.
4. Start Docker containers using `docker-compose`.
5. Prepare the database by running the setup script (`_prepare_database.py`).
6. Ingest data into DataHub from a YAML configuration file (`inventory.yml`).
7. Run the main application (`app/app.py`).


#### Notes

- To run the `setup.sh` script, use the following command:
  ```bash
  chmod +x setup.sh

- You can monitor the process through the shell logs.

Once `setup.sh` finishes execution, a data quality report for the products table will be generated. This indicates that the setup is complete, and you can proceed to the datahub observation phase.

## 2. DataHub Observations

Once the application has finished running, the table quality report will be ready. It can be observed through the following screenshots:

![Custom assertions in the Quality section](docs/custom_assertions.png)
*Custom assertions within the Quality section of the target table.*

![Validation results in assertion details](docs/assertion_details.png)
*The application displays validation results(count) for custom assertions.*

# **Evicting the Resources** 
## Running `nuke.sh`

The `nuke.sh` script is designed for evicting the resources and tearing down the project environment. It performs the following steps:

1. Stops and removes Docker containers, volumes, and orphaned containers.
2. Deletes the virtual environment.

#### Notes

- To run the `nuke.sh` script, use the following command:
  ```bash
  chmod +x nuke.sh
- You can monitor the process through the shell logs.
Once nuke.sh finishes execution, the environment and all associated resources will be removed!
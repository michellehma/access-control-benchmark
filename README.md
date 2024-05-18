# access-control-benchmark

Running this code would require:

- MySQL to be installed and running,
- an abac, rbac, and pbac database to exist with the correct tables (see ER diagram),
- TPC-H data to be generated and loaded into a MySQL database titled "business",
- engine connections and file paths in the "LOAD DATA" operation in both programs to be changed to fit your sql connection and file locations

policies.py should be ran first if you want to execute queries with access control. 

# Access Control Models Implementation(Alternate approach)

This repository contains implementations of three different access control models: Attribute-Based Access Control (ABAC), Policy-Based Access Control (PBAC), and Role-Based Access Control (RBAC). These models are designed to simulate various security scenarios to demonstrate how different access control strategies can be applied across various conditions.

## Key Points
- **Each file contains 3 sections of code**:
  - **Policy Generator**: Designed to create one policy at a time but can be easily modified to generate multiple policies.
  - **CSV File Printing**: For printing the contents of CSV files which log the access decisions and policies.
  - **Clearing CSV Files**: Functionality to clear all the CSV files to reset the data as needed.
- **Manual Crafting of Scenarios**: All scenarios are manually crafted based on identifying correlations in the TPCH database and roles are based on realistic industry roles.

## File Descriptions

### ABAC.ipynb
**Description**: Implements the Attribute-Based Access Control (ABAC) model using Python to simulate access control decisions based on user attributes, environmental conditions, and resource requirements.

**Key Features**:
- Dynamic access control based on multiple attributes.
- Simulation of environmental factors influencing access decisions.
- Data logging to CSV files for access decisions and policies.

### PBAC (1).ipynb
**Description**: Demonstrates Policy-Based Access Control (PBAC), focusing on the enforcement of complex organizational policies.

**Key Features**:
- Definition and enforcement of granular access policies.
- Evaluation of access requests against predefined policies.
- Detailed policy management and decision logging.

### RBAC (2).ipynb
**Description**: Implements Role-Based Access Control (RBAC) where access permissions are assigned to roles rather than individual users.

**Key Features**:
- Role definition and role assignment mechanisms.
- Simulated access control decisions based on user roles.
- Role hierarchies and constraints for comprehensive access control.

## Usage Instructions

### Prerequisites:
- Python 3.x
- Libraries: `faker`, `csv`, and other dependencies as needed.
- Jupyter Notebook or an equivalent Python environment to run `.ipynb` files.

### Running the Notebooks:
1. Open each notebook in Jupyter Notebook or your preferred environment.
2. Execute the cells in order to see the simulation of the access control model described within each notebook.

## Scenarios Detail

### 1. Comprehensive Attribute Design (ABAC)
- **Dynamic Attributes**: Uses dynamically set environmental attributes like `current_time`, `current_date`, and `network_security_level` to reflect real-world conditions.
- **Diverse User and Resource Attributes**: Attributes such as `user_clearance`, `business_unit`, and `data_sensitivity` cater to various business roles and data access requirements.

### 2. Role Hierarchies and Permissions (RBAC)
- **Role-Based Access Control**: Defines specific roles with associated access permissions to simulate organizational structures and access policies.
- **Role Hierarchies**: Includes scenarios implying role hierarchies, demonstrating a nuanced approach to access control.

### 3. Policy Constraints and Flexibility (PBAC)
- **Policy Evaluation**: Flexibly evaluates access based on complex conditions beyond just roles or attributes.
- **Granular Control**: Enforces restrictions or grants access based on combinations of factors like time of day and user location.

### 4. Environmental and Contextual Sensitivity
- **Real-time Conditions**: Incorporates conditions like `current_time` or `operational_hours` to dynamically adjust to context.

### 5. Scenario Simulation and Testing
- **Wide Coverage**: Covers a broad spectrum of typical business operations for comprehensive testing.
- **Interactive Testing**: Allows interactive input to continue or stop scenario simulations, providing a hands-on feel for real-time decision impacts.

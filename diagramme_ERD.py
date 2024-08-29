import sqlite3
import pandas as pd
import os

db_path = 'data/bdd_ERD.sqlite'

if os.path.exists(db_path):
    os.remove(db_path)

def creer_base_de_donnees():
    conn = sqlite3.connect(db_path)

    curseur = conn.cursor()

    # Création des tables
    curseur.execute("""
        CREATE TABLE IF NOT EXISTS employe (
            employee_id INTEGER PRIMARY KEY,
            employee_name VARCHAR(255)
        )
    """)
    
    curseur.execute("""
        CREATE TABLE IF NOT EXISTS employe_details (
            employee_id INTEGER PRIMARY KEY,
            date_of_birth DATE,
            social_security_number VARCHAR(11),
            FOREIGN KEY (employee_id) REFERENCES employe (employee_id)
        )
    """)

    curseur.execute("""
        CREATE TABLE IF NOT EXISTS departments (
            department_id INTEGER PRIMARY KEY,
            department_name VARCHAR(255)
        )
    """)

    curseur.execute("""
        CREATE TABLE IF NOT EXISTS employe_departments (
            employee_id INTEGER,
            department_id INTEGER,
            PRIMARY KEY (employee_id, department_id),
            FOREIGN KEY (employee_id) REFERENCES employe (employee_id),
            FOREIGN KEY (department_id) REFERENCES departments (department_id)
        )
    """)

    curseur.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            project_id INTEGER PRIMARY KEY,
            project_name VARCHAR(255)
        )
    """)

    curseur.execute("""
        CREATE TABLE IF NOT EXISTS employe_projects (
            employee_id INTEGER,
            project_id INTEGER,
            PRIMARY KEY (employee_id, project_id),
            FOREIGN KEY (employee_id) REFERENCES employe (employee_id),
            FOREIGN KEY (project_id) REFERENCES projects (project_id)
        )
    """)

    # Insertion des données
    curseur.execute("""
        INSERT INTO employe (employee_id, employee_name) VALUES
        (1, 'John Doe'),
        (2, 'Jane Smith'),
        (3, 'Emily Johnson'),
        (4, 'Michael Brown')
    """)

    curseur.execute("""
        INSERT INTO employe_details (employee_id, date_of_birth, social_security_number) VALUES
        (1, '1985-06-15', '123-45-6789'),
        (2, '1990-09-25', '987-65-4321'),
        (3, '2000-01-20', '456-78-9123'),
        (4, '1987-03-11', '321-54-9876')
    """)

    curseur.execute("""
        INSERT INTO departments (department_id, department_name) VALUES
        (1, 'HR'),
        (2, 'Engineering'),
        (3, 'Marketing')
    """)

    curseur.execute("""
        INSERT INTO employe_departments (employee_id, department_id) VALUES
        (1, 1),
        (2, 2),
        (3, 2),
        (4, 3)
    """)

    curseur.execute("""
        INSERT INTO projects (project_id, project_name) VALUES
        (1, 'Project A'),
        (2, 'Project B'),
        (3, 'Project C')
    """)

    curseur.execute("""
        INSERT INTO employe_projects (employee_id, project_id) VALUES
        (1, 1),
        (1, 2),
        (2, 1),
        (3, 2),
        (3, 3),
        (4, 3)
    """)

    conn.commit()

    conn.close()

def analysis(db_path):
    conn = sqlite3.connect(db_path)
    
    query_projects_per_employee = '''
        SELECT employee_name, COUNT(project_id) AS Total_Projects
        FROM employe_projects
        JOIN employe ON employe_projects.employee_id = employe.employee_id
        GROUP BY employee_name;
    '''
    projects_per_employee = pd.read_sql_query(query_projects_per_employee, conn)

    query_employees_per_department = '''
        SELECT department_name, COUNT(employee_id) AS Total_Employees
        FROM employe_departments
        JOIN departments ON employe_departments.department_id = departments.department_id
        GROUP BY department_name;
    '''
    employees_per_department = pd.read_sql_query(query_employees_per_department, conn)

    print("Nombre total de projets par employé:")
    print(projects_per_employee)

    print("\nNombre d'employés par département:")
    print(employees_per_department)

    conn.close()

if __name__ == "__main__":
    creer_base_de_donnees()
    analysis(db_path)

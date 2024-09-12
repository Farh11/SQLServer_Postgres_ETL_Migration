FROM apache/airflow:2.9.3

RUN pip install markupsafe==2.1.5 \
    && pip install apache-airflow-providers-odbc \
    && pip install pyodbc \
    && pip install 'apache-airflow-providers-microsoft-mssql[odbc]' \
    && pip install apache-airflow-providers-microsoft-azure \
    && pip install gitpython


import sqlalchemy

#Connection details to connect to database


#Functions to create and insert into tables
#write the dataframe into the database (this function deletes the original table and recreates it) watch out for column types in db
def erase_and_create(database_name, table_name, df_name):
    database_name = database_name    
    database_username = '{db-user-name}'
    database_password = '{db-password}'
    database_ip = '{db-ip}'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?autocommit=true'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))
    df_name.to_sql(con=database_connection, name=table_name,
                   if_exists='replace', chunksize=100, index=False)
    database_connection.dispose()                                  

#write the dataframe into the database (this function insert the whole dataframe to the db table)
def insert(database_name, table_name, df_name):
    database_username = '{db-user-name}'
    database_password = '{db-password}'
    database_ip = '{db-ip}'
    database_name = database_name
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}?autocommit=true'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))
    df_name.to_sql(con=database_connection, name=table_name,
                   if_exists='append', chunksize=100, index=False)
    database_connection.dispose()       

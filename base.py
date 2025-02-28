import psycopg2
import pandas as pd
db_host = "database-1.czycqwo2ayk6.us-east-2.rds.amazonaws.com"
db_name = 'database1'
db_user = 'postgres'
db_pass = '#MAC1234'


connection = psycopg2.connect(host = db_host, dbname = db_name, user = db_user, password = db_pass)
print("Conexión exitosa")

cursor = connection.cursor()


#tablas = ['incumplimientos', 'montos_pagados', 'montos_por_pagar', 'estatus_pagos', 'clientes', 'temp_dataset']

#for tabla in tablas:
#    cursor.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")

#connection.commit()
#print("Todas las tablas eliminadas correctamente")


cursor.execute("""
-- Tabla de clientes (permitiendo nulos donde sea necesario)
CREATE TABLE IF NOT EXISTS clientes (
    id_cliente SERIAL PRIMARY KEY,
    limit_bal FLOAT,
    sexo FLOAT,
    education FLOAT,
    marriage FLOAT,
    age FLOAT
);

-- Tabla de estatus de pagos (sin NOT NULL)
CREATE TABLE IF NOT EXISTS estatus_pagos (
    id_pago SERIAL PRIMARY KEY,
    id_cliente INT,
    mes DATE,
    estatus FLOAT,
    CONSTRAINT fk_estatus_pagos_cliente FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);

-- Tabla de montos por pagar (sin NOT NULL)
CREATE TABLE IF NOT EXISTS montos_por_pagar (
    id_monto SERIAL PRIMARY KEY,
    id_cliente INT,
    mes DATE,
    monto FLOAT,
    CONSTRAINT fk_montos_por_pagar_cliente FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);

-- Tabla de montos pagados (sin NOT NULL)
CREATE TABLE IF NOT EXISTS  montos_pagados (
    id_pago SERIAL PRIMARY KEY,
    id_cliente INT,
    mes DATE,
    monto FLOAT,
    CONSTRAINT fk_montos_pagados_cliente FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);

-- Tabla de incumplimientos (sin NOT NULL)
CREATE TABLE IF NOT EXISTS incumplimientos (
    id_incumplimiento SERIAL PRIMARY KEY,
    id_cliente INT,
    default_payment_next_month FLOAT,
    CONSTRAINT fk_incumplimientos_cliente FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);

-- Tabla temporal para cargar el dataset (sin NOT NULL)
CREATE TABLE IF NOT EXISTS temp_dataset (
    limit_bal FLOAT,
    sexo FLOAT,
    education FLOAT,
    marriage FLOAT,
    age FLOAT,
    pay_0 FLOAT,
    pay_1 FLOAT,
    pay_2 FLOAT,
    pay_3 FLOAT,
    pay_4 FLOAT,
    pay_5 FLOAT,            
    bill_amt1 FLOAT,
    bill_amt2 FLOAT,
    bill_amt3 FLOAT,
    bill_amt4 FLOAT,
    bill_amt5 FLOAT,
    bill_amt6 FLOAT,
    pay_amt1 FLOAT,
    pay_amt2 FLOAT,
    pay_amt3 FLOAT,
    pay_amt4 FLOAT,
    pay_amt5 FLOAT,
    pay_amt6 FLOAT,
    default_payment_next_month FLOAT
);        
    
""")

connection.commit()
print("Tablas creadas ")

'''''
cursor.execute("""
ALTER TABLE temp_dataset 
DROP COLUMN IF EXISTS pay_6;
""")
connection.commit()
print("Columna 'pay_6' eliminada correctamente de 'temp_dataset'")



cursor.execute("TRUNCATE TABLE temp_dataset;")
connection.commit()
print("Todos los datos de la tabla 'temp_dataset' han sido eliminados")


df = pd.read_csv('datos_sucios.csv')
df = df.where(pd.notnull(df), None)


total_rows = len(df)  

# Insertar datos en la tabla temporal con un contador de progreso
for i, row in enumerate(df.iterrows(), start=1):
    cursor.execute("""
        INSERT INTO temp_dataset (
            limit_bal, sexo, education, marriage, age, 
            pay_0,pay_1, pay_2, pay_3, pay_4, pay_5,
            bill_amt1, bill_amt2, bill_amt3, bill_amt4, bill_amt5, bill_amt6, 
            pay_amt1, pay_amt2, pay_amt3, pay_amt4, pay_amt5, pay_amt6, 
            default_payment_next_month
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                  %s, %s, %s, %s, %s, %s, 
                  %s, %s, %s, %s, %s, %s, 
                  %s)
    """, tuple(row[1]))
    
    
    if i % 100 == 0 or i == total_rows:
        print(f"Progreso: {i}/{total_rows} filas insertadas ({(i/total_rows)*100:.2f}%)")

connection.commit()

'''''
'''''
try:
    # Insertar en la tabla 'clientes'
    cursor.execute("""
    INSERT INTO clientes (limit_bal, sexo, education, marriage, age)
    SELECT DISTINCT 
        limit_bal, sexo, education, marriage, age 
    FROM temp_dataset;
    """)
    print("Datos insertados en 'clientes'")

    # Insertar en 'estatus_pagos'
    for i, mes in enumerate(['2005-09-01', '2005-08-01', '2005-07-01', '2005-06-01', '2005-05-01', '2005-04-01']):
        cursor.execute(f"""
        INSERT INTO estatus_pagos (id_cliente, mes, estatus)
        SELECT 
            c.id_cliente, 
            '{mes}'::DATE, 
            t.pay_{i}
        FROM temp_dataset t
        JOIN clientes c 
            ON t.limit_bal = c.limit_bal 
            AND t.age = c.age;
        """)
    print("Datos insertados en 'estatus_pagos'")

    # Insertar en 'montos_por_pagar' y 'montos_pagados'
    for i, mes in enumerate(['2005-09-01', '2005-08-01', '2005-07-01', '2005-06-01', '2005-05-01', '2005-04-01']):
        # Montos por pagar
        cursor.execute(f"""
        INSERT INTO montos_por_pagar (id_cliente, mes, monto)
        SELECT 
            c.id_cliente, 
            '{mes}'::DATE, 
            t.bill_amt{i+1}
        FROM temp_dataset t
        JOIN clientes c 
            ON t.limit_bal = c.limit_bal 
            AND t.age = c.age;
        """)
        
        # Montos pagados
        cursor.execute(f"""
        INSERT INTO montos_pagados (id_cliente, mes, monto)
        SELECT 
            c.id_cliente, 
            '{mes}'::DATE, 
            t.pay_amt{i+1}
        FROM temp_dataset t
        JOIN clientes c 
            ON t.limit_bal = c.limit_bal 
            AND t.age = c.age;
        """)
    print("Datos insertados en 'montos_por_pagar' y 'montos_pagados'")

    # Insertar en 'incumplimientos'
    cursor.execute("""
    INSERT INTO incumplimientos (id_cliente, default_payment_next_month)
    SELECT 
        c.id_cliente, 
        t.default_payment_next_month
    FROM temp_dataset t
    JOIN clientes c 
        ON t.limit_bal = c.limit_bal 
        AND t.age = c.age;
    """)
    print("Datos insertados en 'incumplimientos'")

    
    connection.commit()
    print("¡Todos los cambios se han guardado exitosamente!")

except Exception as e:
   
    connection.rollback()
    print(f"Error durante la carga de datos: {e}")

'''''
'''''
# Consultas
# Tabla clientes
cursor.execute("SELECT * FROM clientes LIMIT 10;")
print("Clientes:", cursor.fetchall())

# Tabla estatus_pagos
cursor.execute("SELECT * FROM estatus_pagos LIMIT 10;")
print("Estatus de pagos:", cursor.fetchall())

# Tabla montos_pagados
cursor.execute("SELECT * FROM montos_pagados LIMIT 10;")
print("Montos pagados:", cursor.fetchall())

# Tabla montos_por_pagar
cursor.execute("SELECT * FROM montos_por_pagar LIMIT 10;")
print("Montos por pagar:", cursor.fetchall())

# Tabla incumplimientos
cursor.execute("SELECT * FROM incumplimientos LIMIT 10;")
print("Incumplimientos:", cursor.fetchall())

'''''




# Cerrar la conexión
cursor.close()
connection.close()













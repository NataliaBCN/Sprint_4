CREATE SCHEMA `transactions_new` DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ;
-- utf8 nos permite usar caracteres con acentos (como Québec City)

USE  transactions_new;

-- Creamos las tablas y insertamos en ellas datos de los archivos .csv 
CREATE TABLE IF NOT EXISTS  companies (
        company_id VARCHAR(6) PRIMARY KEY ,
        company_name VARCHAR(50),
        phone VARCHAR(15),
        email VARCHAR(50),
        country VARCHAR(50),
        website varchar(50)
	);

-- permitimos data loading    
SET GLOBAL local_infile = 1;

LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
IGNORE 1 LINES;

CREATE TABLE IF NOT EXISTS  transactions (

		id VARCHAR(50) PRIMARY KEY ,
		card_id VARCHAR(15),
		business_id VARCHAR(6),
		timestamp  TIMESTAMP,
		amount  DECIMAL(10, 2),
		declined  BOOLEAN,
		product_ids VARCHAR(25),
		user_id VARCHAR(6),
		lat  VARCHAR(25), 
		longitude VARCHAR(25)
    );

LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';' -- separador diferente de las otras tablas 
ENCLOSED BY '"'
IGNORE 1 LINES;
 
CREATE TABLE IF NOT EXISTS  credit_cards (
		id VARCHAR(15) PRIMARY KEY ,
		user_id VARCHAR(6),
		iban VARCHAR(50),
		pan VARCHAR(50),
		pin CHAR(4),
		cvv CHAR(3),
		track1 VARCHAR(50),
		track2 VARCHAR(50),
		expiring_date varchar(10)
        );

LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ',' -- separador coma
ENCLOSED BY '"'
IGNORE 1 LINES;

CREATE TABLE IF NOT EXISTS  products (
		id VARCHAR(5) PRIMARY KEY ,
		product_name VARCHAR(50),
		price VARCHAR(25),
		colour VARCHAR(25),
		weight FLOAT,
		warehouse_id varchar(6)
        );

LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' -- separador coma
ENCLOSED BY '"'
IGNORE 1 LINES;

CREATE TABLE IF NOT EXISTS  users (
		id VARCHAR(5) PRIMARY KEY ,
        name VARCHAR(50),
		surname VARCHAR(50),
		phone VARCHAR(25),
		email VARCHAR(50),
		birth_date VARCHAR(20),
		country VARCHAR(25),
		city VARCHAR(25), 
		postal_code VARCHAR(15),
		address VARCHAR(100)
        );

-- el campo address en algunos casos contiene coma. 
-- Tenemos la  condicion ENCLOSED BY '"' y añadimos un marcador del fin del registro LINES TERMINATED BY '\r\n'  
-- \r\n is a newline and a carriage return. Used as a new line character in Windows

LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/users_ca.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' -- separador coma
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  
IGNORE 1 LINES
;
LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/users_usa.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' -- separador coma
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
;
LOAD DATA   INFILE '/usr/local/mysql-8.0.21-macos10.15-x86_64/data/transactions_new/users_uk.csv'
INTO TABLE users
FIELDS TERMINATED BY ',' -- separador coma
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
;

SET FOREIGN_KEY_CHECKS=0;

ALTER TABLE transactions
add foreign key(card_id) references credit_cards(id),
add foreign key(business_id) references companies(company_id),
add foreign key(user_id) references users(id);


-- *** NIVEL 1 *** EJ 1 ***
-- Usuarios con 30 y mas transacciones 
SELECT u.*, count(t.id) as num_transactions
FROM transactions t 
		left outer join users u on t.user_id=u.id
-- WHERE declined=0
GROUP BY u.id
HAVING num_transactions>=30
;

-- *** NIVEL 1 *** EJ 2 ***
SELECT c.company_name, cc.iban, round(avg(t.amount),2) as avg_transaction
FROM transactions t 
		left outer join companies c on t.business_id=c.company_id
		left outer join credit_cards cc on t.card_id=cc.id
WHERE c.company_name='Donec Ltd'
GROUP BY c.company_name,cc.iban
;

-- *** NIVEL 2 *** EJ 1 ***
CREATE TABLE IF NOT EXISTS cc_state 
(card_id VARCHAR(10) PRIMARY KEY,
cc_state VARCHAR(20)
)
AS 
-- si la targeta tiene tres ultimas transacciones declinadas le ponemos 'No activa', al contrario 'Activa'
SELECT  card_id,
		if (sum(declined)>=3,'No activa', 'Activa') as cc_state
FROM (

-- esta subquery es el primer paso 
-- para cada tarjeta de credito numeramos sus transaciones de mas reciente (numero 1) a mas antigua (numero n)
-- para eso usamos dos funcciones ROW_NUMBER (para numerar) y PARTITION BY (para empezar deneracion de nuevo para cada card_id)
SELECT  card_id, 
		CAST(timestamp AS DATE) ,
		declined, ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp desc) as 'rn_orderdate'
FROM transactions
	) a
-- aqui elegimos solo las tres transaciones mas recientes 
WHERE rn_orderdate<4 
GROUP BY card_id;

ALTER TABLE cc_state
ADD FOREIGN KEY (card_id) references credit_cards(id);

SELECT cc_state,
		count(card_id) as num_credit_cards
FROM cc_state
WHERE cc_state='Activa'
;
-- *** NIVEL 3 *** EJ 1 ***
-- creamos una tabla temporal que consiste de un campo con los numeros de 1 a 10
-- CTE (common table expression) create temporary result set
-- RECURSIVE for generating serie of numbers
CREATE TEMPORARY TABLE numbers  WITH RECURSIVE  cte AS 
( SELECT 1 as n
  UNION ALL
  SELECT n +1
  FROM cte -- refers to itself to repeatedly add new values
  WHERE n < 10 -- termination condition 
)
SELECT * FROM cte;

CREATE TABLE IF NOT EXISTS product_sold 
(transaction_id VARCHAR(50),
product_id VARCHAR(5),
PRIMARY KEY (transaction_id, product_id),
FOREIGN KEY (transaction_id) references transactions(id),
FOREIGN KEY (product_id) references products(id));

INSERT INTO product_sold (transaction_id, product_id)
SELECT  id as transaction_id,
	trim(substring_index( substring_index(product_ids, ',', n), ',', -1 )) as product_id 
FROM transactions join numbers 
	on  char_length(product_ids) - char_length(replace(product_ids, ',', ''))+1 >= n 
;

-- el numero de ventas de cada producto
SELECT p.id, product_name, count(ps.transaction_id) as num_sold
from  product_sold ps
inner join products p on product_id = p.id
inner join transactions t on transaction_id=t.id
where declined=0
group by p.id, product_name
order by num_sold desc;


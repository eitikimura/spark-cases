////////////////
//Carga de dados
////////////////

//Imports para usar as funções de sort e para que hajam as conversões implícitas para trabalhar com os integers 
import org.apache.spark.sql.functions._
import spark.implicits._

//Carga do arquivo jSON para um dataframe
val df = spark.read.json("examples/src/main/resources/people.json")
//val df = spark.read.json("/Volumes/PANZER/Github/spark-cases/people.json")

//Vamos ver o que tem dentro do nosso arquivo jSON
df.show()

//Para checar os datatypes, basta usar o método do dataframe chamado .printSchema()
df.printSchema()



////////////////////////
//Operações com Untyped 
////////////////////////

//Selecionando apenas a coluna nome
df.select("name").show()

//Seleciona as colunas "name" e "age". No final criamos uma coluna a mais com a soma de 1 na idade. 
df.select($"name", $"age", $"age" + 1).show()

//Filtra pessoas com mais de 21 anos
df.filter($"age" > 21).show()

//VVamos filtrar as pessoas com mais de 21 anos, e com idade igual ou menor do que 35
df.filter($"age" > 21 && $"age" <= 35).show()

//Agrupa via contagem todas as pessoas pela idade
df.groupBy("age").count().show()

//Formas de realizar ordenação
df.groupBy("age").count().orderBy(desc("count")).show()

df.groupBy("age").count().sort($"count".desc).show()



////////////////////
//Operações com SQL
////////////////////

//Instanciamento de view temporária usando o método .createOrReplaceTempView() a qual vamos chamar de "people"
df.createOrReplaceTempView("people")

//Vamos fazer um select simples nessa view que acabamos de criar
spark.sql("SELECT * FROM people").show()

//Instanciamento de view global
df.createGlobalTempView("people")

// Consulta na view atrelado em uma base de dados no banco de dados 'global.temp'
spark.sql("SELECT * FROM global_temp.people").show()

// Com a consulta abaixo criamos uma sessão nova, e mesmo assim conseguimos acessar a view. 
spark.newSession().sql("SELECT * FROM global_temp.people").show()

//Um uso simples do group by 
spark.newSession().sql("SELECT age, count(*) AS qty FROM global_temp.people GROUP BY age").show()
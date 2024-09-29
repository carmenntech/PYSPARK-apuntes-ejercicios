# Apuntes PYSPARK

## Funciones importantes 

### Funciones de metadatos

`df.show(5)` Muestra las primeras 5 filas

`df.printSchema()` Muestra un esquema con las columnas y los tipos de datos del ddr

`df.colums` Muestra los nombres de las columnas 

`df.stype` Muestra los tipos de las columnas 

### Funciones de selección 

`df.select(data.edad).show() | show(Truncate=False)`
 Seleccionar solo una columna

`df.select(data['edad']).show()`
 Seleccionar solo una columna

`df.select(data.edad, data.urn_id).show(Truncate=False)`
 Seleccionar varias columnas

### Agregar nuevas columnas

```
from pyspark.sql.fuctions import lit

df = data.withColumn('First_Column', lit(1)) \
      .withColumn('Second_Column', lit(2))
```

### Operaciones de columnas 

`df.gorupBy("location", "edad").count().show()` Agrupar por columna

`df = df.drop('Second-column', 'Third-column')` Eliminar columna

`df.orderBy('Primera-columna').show()` Ordenar las filas por la primera columna

`df.groupBy('Origin').count().orderBy('count', ascending=False).show()`


### Filtrados

Filtrar por una columna:

`madrid = df.filter(col('location')=="Madrid").show(truncate=False)` 

Filtrar por varias columnas:

```
madrid_18 = df.filter((col('location')=="Madrid")&(col('location')=="Madrid")).show(truncate=False)
```

### Primeros Pasos

1- Configuramos nuestra 'granja' de spark, el numero de ordenadores, sus ips... en este caso es un solo ordenador local, después creamos un spark context

![image](https://github.com/user-attachments/assets/c7adca78-77e8-47d8-a5cf-4b51144aaa83)

2- Creamos un RDD con testFile

3- Transformamos con map los datos (Funcion de transformacion)

4- Usamos la funcion countByValue() para contar los valores por cada clave (Funcion de accion)

![image](https://github.com/user-attachments/assets/618d9e84-7d84-4ddf-8307-438bff335a64)

![image](https://github.com/user-attachments/assets/c2f72e2b-2d00-46b6-90ce-567705853abf)

5- Ordenamos los datos con sorted y los mostramos por calve, valor con for

![image](https://github.com/user-attachments/assets/df352ae5-6e73-4bc9-b263-eda75261aa55)


### Fuciones de transformacion 
+ __map__ Permite tomar un set de datos y transfomarlo en otro dada una funcion (lambda)
+ __filter__ Filtra los datos que no quieres que sean procesados en el set de datos
+ __flatMap__ Es la misma funcion que map() pero puede devolver varios set de datos 
+ __mapPartitions__ plica una función a cada partición del conjunto de datos en lugar de aplicarla a cada elemento individualmente
+ __sample__  para realizar pruebas
+ __union__  Combina dos conjuntos de datos en uno solo, incluyendo todos los elementos de ambos conjuntos (sin eliminar duplicados).
+ __intersection__ Devuelve los elementos comunes entre dos conjuntos de datos, es decir, la intersección de ambos.
+ __distinct__ Obtiene los datos especificos de un set de datos
+ __groupByKey__ Agrupa los elementos de un conjunto de datos que tienen la misma clave, generando pares de clave y un conjunto de valores correspondientes a esa clave.

### Fuciones de acción

+ __reduce__ Aplica una función de reducción (como una suma o multiplicación) a los elementos del conjunto de datos, combinándolos en un solo valor
+ __collect__ Recoge todos los elementos de un conjunto de datos y los devuelve al controlador como una lista
+ __count__ Devuelve el número total de elementos en el conjunto de datos.
+ __first__ Devuelve el primer elemento del conjunto de datos.
+ __take__ Devuelve un número específico de elementos del conjunto de datos
+ __foreach__ Aplica una función a cada elemento del conjunto de datos, pero no devuelve ningún valor (normalmente utilizado para acciones que tienen efectos secundarios como escribir en una base de datos).
+ __countByKey__ Cuenta el número de elementos en cada grupo para cada clave en un conjunto de datos de pares clave-valor
+ __countByValue__ Cuenta el número de ocurrencias de cada valor en el conjunto de datos.
+ __saveAsTextFile__ Guarda el conjunto de datos como un archivo de texto en el sistema de archivos especificado.

### DIferencias flatmap() y map()

__map()__ 

Transforma cada elemento de un RDD a un nuevo elemento 

Un input y un output

Ejemplo:

![image](https://github.com/user-attachments/assets/57fd3c64-5bd6-402b-bbb9-c14a66a7fae5)

__flatmap()__

Transforma cada elemento de un RDD en uno o varios elementos 

Un input y varios output

Ejemplo:

![image](https://github.com/user-attachments/assets/2a567b9d-dd9a-4c23-8c5e-76243b0cf962)


__Variables Broadcast__

Las variables broadcast en PySpark son una forma eficiente de compartir datos entre todos los nodos del clúster. Cuando tienes un conjunto de datos relativamente pequeño (como una lista o un diccionario) que es constante y quieres que esté disponible en cada nodo sin ser enviado repetidamente, puedes usar una variable broadcast.

En lugar de enviar la variable a cada nodo con cada operación, Spark distribuye la variable a los nodos una vez y la almacena en caché, lo que reduce la sobrecarga de comunicación.


__Conteo de ocurrencias__

Pasos para contar cuantas veces aparece una tecnologia por linea

1) Mapear input data a (itemTecnologia, #ocurrencias) por linea
2) Sumar co-ocurrencias por tecnologia usando reduceByKey()
3) invertir mapeo RDD a (#ocurrencias, itemTecnologia)
4) Buscamos el itemTecnologia y mostramos resultados



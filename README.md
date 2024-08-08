
### Pasos

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
+ __map__ Permite tomar un set de datos y transfomarlo en otro dada una funcion (lamda)
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


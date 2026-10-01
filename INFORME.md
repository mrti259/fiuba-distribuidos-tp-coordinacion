# Informe

## Funcionamiento general

El sistema recibe los datos de los clientes a través de un Gateway, que le asigna un `client_id` a cada cliente y encola los mensajes en una cola común.

## Coordinación

Mediante _Round Robin_, los mensajes se reparten entre las instancias de `Sum`, que acumulan cantidades parciales de fruta de cada cliente. Cuando una instancia de `Sum` detecta que un cliente envió una señal de finalización, notifica a las demás instancias mediante un exchange. Esto permite que todas las instancias envíen sus resultados a las instancias de `Aggregation` y liberen la memoria utilizada para ese cliente.

Los resultados de `Sum` llegan a una instancia de `Aggregation` mediante un exchange elegido a partir de un hash determinista de la fruta. Esto permite que siempre la misma fruta sea procesada por la misma instancia. Cada `Aggregation` combina los resultados parciales recibidos desde las instancias de `Sum` y calcula un top parcial.

El `Join` recibe los tops parciales de todas las instancias de `Aggregation` mediante una cola, combina los resultados, ordena las frutas y conserva las primeras posiciones según el tamaño configurado del top. Finalmente, envía un único resultado al Gateway para que se lo entregue al cliente correspondiente.

## Escalabilidad

### Clientes

Varios clientes pueden enviar información al mismo tiempo porque sus mensajes se distribuyen entre las instancias de `Sum`. Cada cliente se identifica con su propio `client_id`, lo que permite mantener sus resultados separados.

### Datos

Las instancias de `Sum` acumulan los datos antes de enviarlos, por lo que se reduce la cantidad de mensajes que deben procesarse en las etapas siguientes, y la distribución por fruta permite que un cliente con muchos datos utilice varias instancias de `Aggregation`.

### Controles

Es posible aumentar la cantidad de instancias de `Sum` y `Aggregation` modificando la configuración del sistema. La cantidad de mensajes de finalización se adapta a la cantidad de instancias activas, por lo que cada etapa puede saber cuándo recibió todos los resultados necesarios.

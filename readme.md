# Funcionalidades e Tecnologias Utilizadas

![alt text](image.png)
- MQTT: Ambos os scripts utilizam o protocolo MQTT (Message Queuing Telemetry Transport), um protocolo de mensageria leve e eficiente, ideal para comunicação em dispositivos limitados ou redes com largura de banda restrita. Isso é indicado pelo uso do pacote github.com/eclipse/paho.mqtt.golang.

- Variáveis de Ambiente: O carregamento de configurações através de um arquivo .env é feito usando o pacote github.com/joho/godotenv, sugerindo a importância de definir variáveis como o endereço do broker MQTT e possivelmente credenciais ou tópicos específicos para publicação e subscrição.

## publisher.go

Publicador MQTT: Este script parece configurar um cliente MQTT para conectar-se a um broker e publicar mensagens. Ele define handlers para eventos de conexão e desconexão, carrega as configurações do arquivo .env, e provavelmente publica mensagens para um tópico específico.

## subscriber.go
Subscritor MQTT: Similarmente, este script configura um cliente MQTT para receber mensagens de um tópico específico. Ele define um message handler para processar mensagens recebidas, além de handlers para conexão e perda de conexão.

## Uso 

- Para iniciar o publicador, execute:

```
go run publisher.go

```

- Para iniciar o subscritor, execute:

```
go run subscriber.go
```
Certifique-se de que o broker MQTT esteja acessível e que as configurações no arquivo .env estejam corretas.


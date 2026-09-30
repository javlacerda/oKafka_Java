# oKafka

Example of integrating **Omnis Studio** with **Apache Kafka**, including optional support for **Schema Registry**.

## Contents

1. [Running Apache Kafka with Docker](#1-running-apache-kafka-with-docker)
2. [Using Apache Kafka from Omnis](#2-using-apache-kafka-from-omnis)
3. [Using Apache Kafka with Schema Registry](#3-using-apache-kafka-with-schema-registry)

---

## 1. Running Apache Kafka with Docker

Pull the image and create the broker container:

```bash
docker pull apache/kafka:latest
docker run -d -p 9092:9092 --name kafkabroker apache/kafka:latest
```

Once the container exists, start and stop it with:

```bash
docker start kafkabroker
docker stop kafkabroker
```

### Testing the broker from the CLI

Open a shell inside the container:

```bash
docker exec --workdir /opt/kafka/bin/ -it kafkabroker sh
```

Then try the following commands:

```bash
# Create a topic
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic

# List topics
./kafka-topics.sh --bootstrap-server localhost:9092 --list

# Produce messages (type a line and press Enter; Ctrl+C to exit)
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic

# Consume messages from the beginning of the topic
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

---

## 2. Using Apache Kafka from Omnis

### 2.1 Prerequisites

| Requirement | Purpose | Check with |
|---|---|---|
| Java (JDK) | Build and run the OKafka component | `java -version` |
| Apache Maven | Build the project | `mvn -version` |
| Java IDE with Maven support (optional) | Eclipse, IntelliJ IDEA, VS Code, … | — |
| Omnis Studio | Run the demo library | — |
| A running Kafka broker | See [section 1](#1-running-apache-kafka-with-docker) | `docker ps` |

> A JDK is required to build the project; a JRE alone is not enough.

### 2.2 Build `OKafka.jar`

**Option A — command line**

From the project root (the folder containing `pom.xml`):

```bash
mvn clean package
```

**Option B — IDE**

1. Import the project as an existing Maven project (in Eclipse: *File → Import → Maven → Existing Maven Projects*).
2. Run the Maven goals `clean package` (in Eclipse: right-click the project → *Run As → Maven build…*).

In both cases the generated JAR is placed in the `target/` folder. If the file name includes a version suffix, rename it to `OKafka.jar`.

### 2.3 Install the JAR in Omnis

Copy `OKafka.jar` to the following folder inside the Omnis application directory, creating the `okafka` folder if it does not exist:

```
<Omnis Application Directory>/pyworker/okafka/
```

Windows example:

```
C:\Program Files\Omnis Software\<Omnis version>\pyworker\okafka\OKafka.jar
```

> Restart Omnis Studio after copying the file so the component is picked up.

### 2.4 Run the demo

1. Make sure the Kafka broker is running (`docker start kafkabroker`).
2. Open the library **`KAFKADEMO.LBS`** in Omnis Studio.
3. Use the demo windows to produce and consume messages on `localhost:9092`.

### 2.5 Troubleshooting

- **Connection refused / timeout** — confirm the container is running (`docker ps`) and that port `9092` is not in use by another process.
- **Component not found in Omnis** — check that `OKafka.jar` is in `pyworker/okafka/` with that exact name, and restart Omnis.
- **Maven build fails** — confirm `JAVA_HOME` points to a JDK and that `mvn -version` reports the expected Java version.

---

## 3. Using Apache Kafka with Schema Registry

To run Kafka together with Schema Registry, go to the folder containing `docker-compose.yaml` and start the services:

```bash
docker compose up -d
```

To stop and remove the containers:

```bash
docker compose down
```

> If the `kafkabroker` container from section 1 is running, stop it first (`docker stop kafkabroker`) to avoid a port conflict on `9092`.

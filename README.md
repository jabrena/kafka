# Kafka Client State Machines

## Overview

This project provides comprehensive UML state machine diagrams for Apache Kafka clients, documenting their behavior, lifecycle, and state transitions during message operations.

## Files

### Producer Client
- `kafka-producer-state-machine.puml` - PlantUML producer state machine diagram
- `kafka-producer-documentation.md` - Detailed producer state machine analysis

### Consumer Client
- `kafka-consumer-state-machine.puml` - PlantUML consumer state machine diagram
- `kafka-consumer-documentation.md` - Detailed consumer state machine analysis

### General
- `README.md` - This overview document

## State Machine Diagrams

### Kafka Producer Client
The producer state machine illustrates the complete lifecycle from initialization to shutdown:

![Kafka Producer State Machine](kafka-producer-state-machine.puml)

**Key Producer States:**
- **Initialized**: Producer created with configuration loaded
- **Connecting**: Establishing connection to Kafka cluster  
- **Ready**: Connected and ready for message operations
- **Sending**: Actively processing message send operations
- **Failed States**: Various error conditions with recovery paths
- **Closed**: Producer shutdown and resource cleanup

### Kafka Consumer Client
The consumer state machine shows the consumption lifecycle including group coordination:

![Kafka Consumer State Machine](kafka-consumer-state-machine.puml)

**Key Consumer States:**
- **Created**: Consumer instantiated with configuration
- **Subscribed/Assigned**: Topic subscription or manual partition assignment
- **JoiningGroup**: Consumer group coordination and discovery
- **Rebalancing**: Partition rebalancing and assignment
- **Polling**: Active polling state ready for consumption
- **Fetching**: Actively fetching records from brokers
- **Processing**: Processing fetched records
- **Committing**: Committing processed record offsets
- **Closed**: Consumer shutdown and cleanup

## Usage

To render the PlantUML diagrams:

```bash
# Render producer state machine
plantuml kafka-producer-state-machine.puml

# Render consumer state machine  
plantuml kafka-consumer-state-machine.puml

# Render both diagrams
plantuml *.puml
```

Or use the online PlantUML editor at http://www.plantuml.com/plantuml/uml/

## Configuration Impact

### Producer Configuration
Key Kafka producer configurations affecting state transitions:

- `bootstrap.servers`: Initial broker discovery
- `retries`: Send failure recovery attempts
- `request.timeout.ms`: Network operation timeouts
- `batch.size`: Message batching thresholds
- `linger.ms`: Batch accumulation timing

### Consumer Configuration
Key Kafka consumer configurations affecting state transitions:

- `group.id`: Consumer group identifier
- `session.timeout.ms`: Group session timeout
- `heartbeat.interval.ms`: Heartbeat frequency
- `max.poll.interval.ms`: Maximum processing time
- `enable.auto.commit`: Automatic offset commit
- `auto.offset.reset`: Initial offset strategy

## Documentation Standards

This documentation follows @170-java-documentation guidelines with proper PlantUML syntax, business logic alignment, and comprehensive state coverage.
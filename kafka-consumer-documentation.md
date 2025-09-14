# Kafka Consumer Client State Machine Documentation

## Overview

This document describes the UML state machine diagram for the Apache Kafka Consumer client, illustrating its behavior, lifecycle, and state transitions during message consumption operations, including consumer group management and partition rebalancing.

## State Machine Analysis

### Primary States

#### 1. **Created**
- **Purpose**: Initial state after KafkaConsumer instantiation
- **Characteristics**: 
  - Configuration loaded and validated
  - No topic subscriptions or partition assignments
  - Consumer group not joined
- **Entry Actions**: Validate configuration, initialize internal structures
- **Valid Transitions**: → Subscribed, → Assigned, → Closed

#### 2. **Subscribed**
- **Purpose**: Consumer subscribed to topics using consumer group
- **Characteristics**:
  - Topics specified for automatic partition assignment
  - Consumer group protocol enabled
  - Waiting for first poll() to trigger group join
- **Entry Actions**: Store topic subscriptions, prepare for group join
- **Valid Transitions**: → JoiningGroup, → Closed

#### 3. **Assigned**
- **Purpose**: Manual partition assignment without consumer group
- **Characteristics**:
  - Specific partitions manually assigned
  - No consumer group coordination
  - Direct partition ownership
- **Entry Actions**: Store partition assignments
- **Valid Transitions**: → Polling, → Closed

#### 4. **JoiningGroup**
- **Purpose**: Joining consumer group and discovering coordinator
- **Characteristics**:
  - Group coordinator discovery in progress
  - Join group request sent
  - Heartbeat thread initialization
- **Entry Actions**: Discover coordinator, send join group request
- **Valid Transitions**: → Rebalancing, → JoinFailed, → Closed

#### 5. **JoinFailed**
- **Purpose**: Failed to join consumer group
- **Characteristics**:
  - Coordinator unavailable or timeout
  - Authentication/authorization failures
  - Network connectivity issues
- **Entry Actions**: Log error, prepare for retry
- **Valid Transitions**: → JoiningGroup (retry), → Closed

#### 6. **Rebalancing**
- **Purpose**: Partition rebalancing in progress
- **Characteristics**:
  - Partition revocation and assignment
  - Consumer group membership changes
  - Offset position determination
- **Internal States**:
  - **PartitionRevocation**: Revoking current partitions
  - **PartitionAssignment**: Receiving new partition assignments
  - **OffsetReset**: Determining initial positions
- **Entry Actions**: Execute rebalance callbacks
- **Valid Transitions**: → Polling, → RebalanceFailed, → Closed

#### 7. **RebalanceFailed**
- **Purpose**: Rebalancing process failed
- **Characteristics**:
  - Timeout during rebalance
  - Coordinator failures
  - Generation mismatch errors
- **Valid Transitions**: → Rebalancing (retry), → Closed

#### 8. **Polling**
- **Purpose**: Active polling state ready for consumption
- **Characteristics**:
  - Partitions assigned and ready
  - Heartbeat thread active
  - Ready to fetch records
- **Internal States**:
  - **Heartbeating**: Maintaining group membership
  - **SessionTimeout**: Session timeout exceeded
- **Valid Transitions**: → Fetching, → Paused, → Rebalancing, → Closed

#### 9. **Paused**
- **Purpose**: Consumption temporarily paused
- **Characteristics**:
  - Specific partitions paused
  - Heartbeat maintained
  - No record fetching
- **Entry Actions**: Mark partitions as paused
- **Valid Transitions**: → Polling, → Closed

#### 10. **Fetching**
- **Purpose**: Actively fetching records from brokers
- **Characteristics**:
  - Network requests to partition leaders
  - Record deserialization
  - Offset management
- **Entry Actions**: Send fetch requests to brokers
- **Valid Transitions**: → Processing, → FetchTimeout, → FetchError, → Rebalancing

#### 11. **FetchTimeout**
- **Purpose**: Poll operation timed out without records
- **Characteristics**:
  - No records available within poll timeout
  - Heartbeat still maintained
  - Consumer remains active
- **Valid Transitions**: → Polling, → Closed

#### 12. **FetchError**
- **Purpose**: Error occurred during fetch operation
- **Characteristics**:
  - Broker unavailable or network issues
  - Authentication/authorization errors
  - Topic/partition not found
- **Entry Actions**: Log error, determine if recoverable
- **Valid Transitions**: → Polling (recoverable), → Closed (fatal)

#### 13. **Processing**
- **Purpose**: Processing fetched records
- **Characteristics**:
  - Records returned to application
  - Application logic execution
  - Error handling by application
- **Internal States**:
  - **Deserializing**: Converting bytes to objects
  - **ApplicationLogic**: User code execution
  - **DeserializationError**: Deserialization failure
- **Valid Transitions**: → Committing, → Polling, → Seeking, → Closed

#### 14. **Committing**
- **Purpose**: Committing processed record offsets
- **Characteristics**:
  - Synchronous or asynchronous commit
  - Offset persistence to coordinator
  - Commit callback execution
- **Entry Actions**: Send offset commit request
- **Valid Transitions**: → CommitSuccess, → CommitFailed

#### 15. **CommitSuccess**
- **Purpose**: Offset commit completed successfully
- **Characteristics**:
  - Offsets persisted to coordinator
  - Commit callback executed
  - Progress tracking updated
- **Valid Transitions**: → Polling, → Closed

#### 16. **CommitFailed**
- **Purpose**: Offset commit failed
- **Characteristics**:
  - Coordinator unavailable
  - Retriable or non-retriable errors
  - Commit callback with error
- **Entry Actions**: Determine retry strategy
- **Valid Transitions**: → Committing (retry), → Polling, → Closed

#### 17. **Seeking**
- **Purpose**: Manually repositioning consumer offset
- **Characteristics**:
  - seek(), seekToBeginning(), seekToEnd() operations
  - Offset position override
  - Reset consumption position
- **Entry Actions**: Set new partition positions
- **Valid Transitions**: → Polling, → Closed

#### 18. **Closed**
- **Purpose**: Consumer shutdown and cleanup
- **Characteristics**:
  - Leave consumer group
  - Close network connections
  - Release resources
- **Entry Actions**: Leave group, cleanup resources
- **Valid Transitions**: → [*] (terminal state)

### State Transition Triggers

#### Events and Actions

1. **new KafkaConsumer()**: Create consumer instance
2. **subscribe(topics)**: Subscribe to topics with consumer group
3. **assign(partitions)**: Manually assign partitions
4. **poll(timeout)**: Poll for records with timeout
5. **pause(partitions)**: Pause consumption for partitions
6. **resume(partitions)**: Resume consumption for partitions
7. **commitSync()/commitAsync()**: Commit offsets
8. **seek(partition, offset)**: Manually set offset position
9. **close()**: Shutdown consumer gracefully
10. **rebalance triggered**: Partition reassignment needed
11. **session timeout**: Heartbeat session expired
12. **coordinator response**: Group coordinator communication

#### Guard Conditions

- **[retry < maxRetries]**: Retry attempts available
- **[retry >= maxRetries]**: Maximum retries reached
- **[recoverable error]**: Error can be retried
- **[fatal error]**: Permanent error condition
- **[retriable error]**: Commit can be retried
- **[non-retriable error]**: Commit cannot be retried
- **[session.timeout.ms exceeded]**: Session timeout reached

### Configuration Parameters Affecting State Transitions

#### Consumer Group Parameters
- `group.id`: Consumer group identifier
- `session.timeout.ms`: Group session timeout
- `heartbeat.interval.ms`: Heartbeat frequency
- `max.poll.interval.ms`: Maximum processing time

#### Fetch Parameters
- `fetch.min.bytes`: Minimum fetch size
- `fetch.max.wait.ms`: Maximum fetch wait time
- `max.partition.fetch.bytes`: Maximum partition fetch size
- `max.poll.records`: Maximum records per poll

#### Offset Management Parameters
- `enable.auto.commit`: Automatic offset commit
- `auto.commit.interval.ms`: Auto commit frequency
- `auto.offset.reset`: Initial offset strategy
- `isolation.level`: Transaction isolation

#### Retry and Timeout Parameters
- `retry.backoff.ms`: Retry delay
- `request.timeout.ms`: Request timeout
- `connections.max.idle.ms`: Connection idle timeout
- `reconnect.backoff.ms`: Reconnection delay

### Consumer Group Coordination

#### Group Management States
1. **Group Discovery**: Finding group coordinator
2. **Join Group**: Joining consumer group
3. **Sync Group**: Synchronizing group state
4. **Heartbeat**: Maintaining group membership
5. **Leave Group**: Graceful group departure

#### Rebalancing Process
1. **Revoke Partitions**: Release current assignments
2. **Join Group**: Rejoin with updated membership
3. **Assign Partitions**: Receive new assignments
4. **Reset Positions**: Determine starting offsets

### Error Handling and Recovery

#### Connection Recovery
- Automatic reconnection with exponential backoff
- Coordinator rediscovery
- Metadata refresh on failures

#### Rebalance Recovery
- Automatic rebalance retry
- Generation mismatch handling
- Coordinator failover support

#### Commit Failure Recovery
- Retriable vs non-retriable error handling
- Commit retry with backoff
- Offset reset on unrecoverable failures

### Threading Model

The Kafka consumer is single-threaded with background threads:

1. **Main Thread**: Application thread calling consumer methods
2. **Heartbeat Thread**: Maintains group membership
3. **Background Threads**: Metadata refresh and network I/O

### Monitoring and Observability

#### Key Metrics for State Machine Monitoring

- **Group States**: Group membership, rebalance rate
- **Fetch States**: Fetch rate, fetch latency, records consumed
- **Commit States**: Commit rate, commit latency, commit failures
- **Lag Monitoring**: Consumer lag, partition assignment

#### JMX Metrics
- `kafka.consumer:type=consumer-metrics,client-id=*`
- `kafka.consumer:type=consumer-coordinator-metrics,client-id=*`
- `kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*`

### Best Practices

#### Configuration
- Set appropriate session and poll timeouts
- Configure proper offset commit strategies
- Enable idempotent processing where needed

#### Error Handling
- Implement proper exception handling for each state
- Monitor consumer lag and rebalance frequency
- Handle deserialization errors gracefully

#### Resource Management
- Always call close() in finally blocks
- Monitor partition assignment and rebalancing
- Implement proper backpressure handling

## Integration with Java Documentation Standards

This state machine diagram follows the Java documentation guidelines from @170-java-documentation:

- **PlantUML Syntax**: Uses proper PlantUML state machine syntax
- **Business Logic Alignment**: States reflect actual Kafka consumer behavior
- **Implementation Consistency**: References actual configuration parameters
- **Comprehensive Coverage**: Includes all significant consumer states
- **Documentation Clarity**: Provides clear explanations and integration guidance
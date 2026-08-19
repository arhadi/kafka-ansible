# Apache Kafka Ansible Automation

Production-oriented Ansible automation for installing, configuring, initializing, validating, and uninstalling an Apache Kafka KRaft cluster on RHEL 9.

The automation is designed for a **3-node Kafka cluster**, while also supporting a smaller **2-node lab deployment** for development and validation.

> Current tested Kafka version: **Apache Kafka 4.3.1** with Scala **2.13**

## Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Directory Structure](#directory-structure)
- [Requirements](#requirements)
- [Preparing the Kafka Archive](#preparing-the-kafka-archive)
- [Inventory](#inventory)
  - [Two-Node Lab Inventory](#two-node-lab-inventory)
  - [Three-Node Cluster Inventory](#three-node-cluster-inventory)
- [Configuration Variables](#configuration-variables)
- [KRaft Architecture](#kraft-architecture)
- [Cluster ID and Directory IDs](#cluster-id-and-directory-ids)
- [Network Ports](#network-ports)
- [Usage](#usage)
  - [Connectivity Test](#connectivity-test)
  - [Syntax Check](#syntax-check)
  - [List Tasks](#list-tasks)
  - [Install Kafka](#install-kafka)
  - [Validate Kafka](#validate-kafka)
  - [Uninstall Kafka](#uninstall-kafka)
- [Role Task Flow](#role-task-flow)
- [Installation Behavior](#installation-behavior)
- [KRaft Initialization Behavior](#kraft-initialization-behavior)
- [Systemd Management](#systemd-management)
- [Replication Settings](#replication-settings)
- [Validation](#validation)
- [Functional Testing](#functional-testing)
- [Idempotency](#idempotency)
- [Uninstall Behavior](#uninstall-behavior)
- [Scaling from Lab to Three Nodes](#scaling-from-lab-to-three-nodes)
- [Troubleshooting](#troubleshooting)
- [Security and Production Recommendations](#security-and-production-recommendations)
- [Tested Lifecycle](#tested-lifecycle)

## Overview

The `kafka` role manages the Kafka lifecycle:

```text
precheck
   |
   v
prerequisites
   |
   v
install
   |
   v
configure
   |
   v
KRaft initialization
   |
   v
systemd
   |
   v
validate
```

Kafka runs without ZooKeeper using **KRaft**.

Each node in the current design runs both roles:

```properties
process.roles=broker,controller
```

The target production topology is three combined broker/controller nodes:

```text
                    Apache Kafka KRaft Cluster

             +-----------------------------+
             |                             |
             |       KRaft Metadata        |
             |          Quorum             |
             |                             |
             +-------------+---------------+
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
     +----------+     +----------+     +----------+
     | kafka-01 |     | kafka-02 |     | kafka-03 |
     | node.id=1|     | node.id=2|     | node.id=3|
     +----------+     +----------+     +----------+
     | Broker   |     | Broker   |     | Broker   |
     | :9092    |     | :9092    |     | :9092    |
     +----------+     +----------+     +----------+
     |Controller|     |Controller|     |Controller|
     | :9093    |     | :9093    |     | :9093    |
     +----------+     +----------+     +----------+
```

A three-controller quorum can tolerate the loss of one controller while retaining a majority.

The current lab has been successfully tested with two nodes.

## Key Features

- RHEL 9 deployment.
- Apache Kafka 4.3.1.
- Scala 2.13 Kafka distribution.
- Java 21.
- ZooKeeper-free KRaft deployment.
- Combined broker/controller nodes.
- Multi-node Ansible inventory.
- Persistent cluster ID.
- Unique KRaft directory ID per controller.
- Kafka 4.x dynamic initial-controller bootstrap syntax.
- Installation under `/app`.
- Persistent Kafka data directory.
- Dedicated Kafka OS user/group.
- Ansible-managed `server.properties`.
- Ansible-managed systemd service.
- Broker listener on TCP 9092.
- Controller listener on TCP 9093.
- Configurable replication settings.
- Idempotent storage formatting protection.
- Cluster-ID mismatch protection.
- Service, process, port, and metadata validation.
- Safe uninstall with configurable data removal.
- Designed for three nodes while usable in a two-node lab.

## Architecture

The role installs Kafka into a version-specific directory:

```text
/app/kafka_2.13-4.3.1
```

and creates:

```text
/app/kafka -> /app/kafka_2.13-4.3.1
```

Runtime data is separated from the installation:

```text
/app/kafka-data
/app/kafka-logs
```

Persistent KRaft identity files are maintained separately:

```text
/app/kafka-cluster.id
/app/kafka-directory.id
```

The cluster ID is identical on all nodes.

The directory ID is unique on every controller.

## Directory Structure

A representative repository structure is:

```text
kafka-ansible/
├── ansible.cfg
├── inventory
│   ├── group_vars
│   │   └── kafka.yml
│   └── hosts.yml
├── playbooks
│   ├── install_kafka.yml
│   └── uninstall_kafka.yml
└── roles
    └── kafka
        ├── defaults
        │   └── main.yml
        ├── files
        │   └── kafka_2.13-4.3.1.tgz
        ├── handlers
        │   └── main.yml
        ├── meta
        │   └── main.yml
        ├── tasks
        │   ├── configure.yml
        │   ├── install.yml
        │   ├── kraft.yml
        │   ├── main.yml
        │   ├── precheck.yml
        │   ├── prerequisites.yml
        │   ├── systemd.yml
        │   ├── uninstall.yml
        │   └── validate.yml
        └── templates
            ├── kafka.service.j2
            └── server.properties.j2
```

Adjust this section if additional task or template files are added later.

## Requirements

### Ansible control node

- Ansible installed.
- Python 3.
- SSH access to all Kafka nodes.
- Privilege escalation to root.
- Kafka archive stored under `roles/kafka/files/`.

The Ansible control node may also be one of the Kafka nodes for lab purposes. The inventory should still use SSH for that host when simulating a dedicated Ansible control node.

### Managed Kafka nodes

- RHEL 9.
- Java 21 or access to repositories containing Java 21.
- SSH access from the Ansible control node.
- Root or sudo privilege.
- Sufficient disk capacity under `/app`.
- TCP connectivity between Kafka nodes.
- TCP 9092 for broker/client communication.
- TCP 9093 for KRaft controller communication.

### RHEL repositories

A clean RHEL node must have access to repositories that provide:

```text
java-21-openjdk-headless
```

Verify with:

```bash
dnf list java-21-openjdk-headless
```

For subscription-managed RHEL systems, ensure the required BaseOS/AppStream repositories are available before deployment.

## Preparing the Kafka Archive

Place the Kafka distribution in:

```text
roles/kafka/files/
```

For the tested release:

```text
roles/kafka/files/kafka_2.13-4.3.1.tgz
```

Verify:

```bash
ls -lh roles/kafka/files/
```

Example:

```text
-rw-r--r-- 1 root root 130M kafka_2.13-4.3.1.tgz
```

The archive should remain in the Ansible repository after installation and uninstall so the cluster can be rebuilt.

## Inventory

Each Kafka node requires:

- a unique inventory hostname;
- its reachable IP address;
- a unique `kafka_node_id`;
- SSH connection information as required.

### Two-Node Lab Inventory

The tested lab topology uses:

```yaml
---
all:
  children:
    kafka:
      hosts:
        kafka-01:
          ansible_host: 10.148.0.3
          kafka_host: 10.148.0.3
          kafka_node_id: 1

        kafka-02:
          ansible_host: 10.148.0.2
          kafka_host: 10.148.0.2
          kafka_node_id: 2
```

If both hosts use the same SSH account, it may be defined at group level:

```yaml
---
all:
  children:
    kafka:
      vars:
        ansible_user: rhel9
      hosts:
        kafka-01:
          ansible_host: 10.148.0.3
          kafka_host: 10.148.0.3
          kafka_node_id: 1

        kafka-02:
          ansible_host: 10.148.0.2
          kafka_host: 10.148.0.2
          kafka_node_id: 2
```

Even if `kafka-02` is the machine where Ansible is executed, using SSH to its IP simulates a dedicated Ansible control node.

### Three-Node Cluster Inventory

Target three-node inventory:

```yaml
---
all:
  children:
    kafka:
      vars:
        ansible_user: rhel9

      hosts:
        kafka-01:
          ansible_host: 10.148.0.3
          kafka_host: 10.148.0.3
          kafka_node_id: 1

        kafka-02:
          ansible_host: 10.148.0.2
          kafka_host: 10.148.0.2
          kafka_node_id: 2

        kafka-03:
          ansible_host: 10.148.0.4
          kafka_host: 10.148.0.4
          kafka_node_id: 3
```

Replace the sample addresses with the actual addresses assigned to the Kafka nodes.

Every node must have a unique:

```yaml
kafka_node_id
```

Do not reuse node IDs.

## Configuration Variables

The main deployment variables are normally defined in:

```text
inventory/group_vars/kafka.yml
```

### Kafka version

```yaml
kafka_version: "4.3.1"
kafka_scala_version: "2.13"

kafka_archive: "kafka_{{ kafka_scala_version }}-{{ kafka_version }}.tgz"
```

### Operating system user

```yaml
kafka_user: kafka
kafka_group: kafka
```

### Installation

```yaml
kafka_install_root: /app

kafka_install_dir: "{{ kafka_install_root }}/kafka-{{ kafka_version }}"
kafka_current_dir: "{{ kafka_install_root }}/kafka"

kafka_data_dir: "{{ kafka_install_root }}/kafka-data"
kafka_log_dir: "{{ kafka_install_root }}/kafka-logs"
```

Depending on the extraction behavior used by the role, the physical extracted Kafka directory may be:

```text
/app/kafka_2.13-4.3.1
```

with `/app/kafka` pointing to it.

Keep the variables and install task aligned with the actual extracted directory name.

### Network

```yaml
kafka_broker_port: 9092
kafka_controller_port: 9093
```

### KRaft roles

```yaml
kafka_process_roles: "broker,controller"
```

### JVM

```yaml
kafka_heap_opts: "-Xms1G -Xmx1G"
```

Increase the heap for production based on workload, partition count, traffic, and available memory.

### Lab replication

For the initial lab configuration:

```yaml
kafka_default_replication_factor: 1
kafka_min_insync_replicas: 1

kafka_offsets_topic_replication_factor: 1

kafka_transaction_state_log_replication_factor: 1
kafka_transaction_state_log_min_isr: 1
```

These values allow Kafka to operate in a reduced-node lab but are not the recommended three-node production settings.

### Three-node replication

For a three-node deployment:

```yaml
kafka_default_replication_factor: 3
kafka_min_insync_replicas: 2

kafka_offsets_topic_replication_factor: 3

kafka_transaction_state_log_replication_factor: 3
kafka_transaction_state_log_min_isr: 2
```

## KRaft Architecture

Kafka 4.3.1 is deployed using KRaft rather than ZooKeeper.

Each node currently uses:

```properties
process.roles=broker,controller
```

Example generated configuration:

```properties
process.roles=broker,controller
node.id=1

listeners=BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=BROKER://10.148.0.3:9092

controller.listener.names=CONTROLLER

controller.quorum.bootstrap.servers=10.148.0.3:9093,10.148.0.2:9093
```

For three nodes this becomes conceptually:

```properties
controller.quorum.bootstrap.servers=10.148.0.3:9093,10.148.0.2:9093,10.148.0.4:9093
```

The template should build this list from the Kafka inventory rather than hard-code it.

## Cluster ID and Directory IDs

KRaft initialization requires persistent identity management.

### Cluster ID

The cluster ID is generated once using:

```bash
/app/kafka/bin/kafka-storage.sh random-uuid
```

It is persisted as:

```text
/app/kafka-cluster.id
```

The first Kafka inventory host acts as the authoritative initialization host.

All nodes receive the same cluster ID.

Example:

```text
8MCyUmjQQRqacrRnp01A1g
```

The exact value is environment-specific.

### Controller directory ID

Kafka 4.x dynamic controller initialization also requires a unique directory ID for each controller.

Each node stores its own value in:

```text
/app/kafka-directory.id
```

Directory IDs must be unique:

```text
kafka-01 directory ID != kafka-02 directory ID
kafka-02 directory ID != kafka-03 directory ID
```

### Initial controller syntax

Kafka 4.x uses:

```text
node-id@host:port:directory-id
```

For example:

```text
1@10.148.0.3:9093:<directory-id-1>,2@10.148.0.2:9093:<directory-id-2>
```

For three nodes:

```text
1@10.148.0.3:9093:<directory-id-1>,2@10.148.0.2:9093:<directory-id-2>,3@10.148.0.4:9093:<directory-id-3>
```

The role dynamically constructs `kafka_initial_controllers` only after all nodes have their directory IDs.

It should not be statically defined in `inventory/group_vars/kafka.yml`.

## Network Ports

| Port | Purpose | Required connectivity |
|---|---|---|
| `9092/tcp` | Kafka broker/client listener | Clients and Kafka nodes as required |
| `9093/tcp` | KRaft controller listener | Between all controller nodes |

For a combined three-node broker/controller cluster, every Kafka node must be able to communicate with every other Kafka node on TCP 9093.

Example:

```text
kafka-01:9093 <----> kafka-02:9093
kafka-01:9093 <----> kafka-03:9093
kafka-02:9093 <----> kafka-03:9093
```

During lab testing, an active RHEL `firewalld` on one node blocked inbound 9093 and caused an asymmetric connectivity failure.

Symptoms included:

```text
Connection to node 1 could not be established
Election timed out before receiving sufficient vote responses
```

Validate both directions when troubleshooting:

```bash
timeout 3 bash -c "</dev/tcp/10.148.0.2/9093"
timeout 3 bash -c "</dev/tcp/10.148.0.3/9093"
```

For production, do not rely on disabling the firewall. Explicitly permit the required traffic and restrict controller access to trusted Kafka nodes.

## Usage

Run commands from the repository root.

### Connectivity Test

First verify Ansible SSH connectivity:

```bash
ansible kafka -m ping
```

Verify privilege escalation:

```bash
ansible kafka -b -m command -a "id"
```

Expected:

```text
uid=0(root) gid=0(root)
```

### Syntax Check

```bash
ansible-playbook \
  playbooks/install_kafka.yml \
  --syntax-check
```

For uninstall:

```bash
ansible-playbook \
  playbooks/uninstall_kafka.yml \
  --syntax-check
```

If your `ansible.cfg` does not define the inventory, specify it explicitly:

```bash
ansible-playbook \
  -i inventory/hosts.yml \
  playbooks/install_kafka.yml \
  --syntax-check
```

### List Tasks

```bash
ansible-playbook \
  playbooks/install_kafka.yml \
  --list-tasks
```

Representative installation flow:

```text
kafka : Run Kafka prechecks
kafka : Uninstall Kafka
kafka : Install Kafka prerequisites
kafka : Install Kafka binaries
kafka : Configure Kafka
kafka : Initialize KRaft
kafka : Configure Kafka systemd service
kafka : Validate Kafka
```

The role may display both install and uninstall include tasks in `--list-tasks`; runtime `when` conditions determine which path executes.

### Install Kafka

```bash
ansible-playbook \
  playbooks/install_kafka.yml \
  -v
```

For more troubleshooting detail:

```bash
ansible-playbook \
  playbooks/install_kafka.yml \
  -vvv
```

Do not limit the initial KRaft bootstrap to only one controller node. Initialize the intended controller set together.

### Validate Kafka

Check service and listeners:

```bash
ansible kafka -b -m shell -a \
'echo "===== $(hostname) ====="; \
systemctl is-active kafka; \
ss -lntp | grep -E ":9092|:9093" || true'
```

Expected on each combined node:

```text
active
*:9092 LISTEN
*:9093 LISTEN
```

### Uninstall Kafka

Standard uninstall:

```bash
ansible-playbook \
  playbooks/uninstall_kafka.yml \
  -v
```

The default uninstall should preserve Kafka data and cluster identity unless the destructive options are explicitly enabled.

For a complete lab reset:

```bash
ansible-playbook \
  playbooks/uninstall_kafka.yml \
  -e kafka_uninstall_remove_data=true \
  -e kafka_uninstall_remove_cluster_metadata=true \
  -e kafka_uninstall_remove_user=true \
  -v
```

Review the uninstall variables before running this against any environment containing required Kafka data.

## Role Task Flow

A clean role dispatcher can use:

```text
kafka_action=install
       |
       +--> precheck.yml
       +--> prerequisites.yml
       +--> install.yml
       +--> configure.yml
       +--> kraft.yml
       +--> systemd.yml
       +--> validate.yml

kafka_action=uninstall
       |
       +--> uninstall.yml
```

Representative `tasks/main.yml` logic:

```yaml
- name: Run Kafka uninstall
  ansible.builtin.include_tasks: uninstall.yml
  when:
    - kafka_action | default('install') == 'uninstall'

- name: Run Kafka installation
  when:
    - kafka_action | default('install') == 'install'
  block:

    - name: Run Kafka prechecks
      ansible.builtin.include_tasks: precheck.yml

    - name: Install Kafka prerequisites
      ansible.builtin.include_tasks: prerequisites.yml

    - name: Install Kafka binaries
      ansible.builtin.include_tasks: install.yml

    - name: Configure Kafka
      ansible.builtin.include_tasks: configure.yml

    - name: Initialize KRaft
      ansible.builtin.include_tasks: kraft.yml

    - name: Configure Kafka systemd service
      ansible.builtin.include_tasks: systemd.yml

    - name: Validate Kafka
      ansible.builtin.include_tasks: validate.yml
```

## Installation Behavior

The installation workflow should:

1. verify the Kafka archive exists;
2. verify required OS prerequisites;
3. install Java 21 when required;
4. create the `kafka` group;
5. create the `kafka` user;
6. create `/app`;
7. create data and log directories;
8. copy the Kafka archive;
9. extract the archive;
10. normalize ownership;
11. create `/app/kafka`;
12. validate Kafka executables;
13. deploy configuration;
14. initialize KRaft only when storage is unformatted;
15. install the systemd unit;
16. start Kafka;
17. validate the resulting service.

A key implementation detail is the directory produced by the Apache archive:

```text
kafka_2.13-4.3.1
```

The role must not assume the archive extracts to:

```text
kafka-4.3.1
```

unless it explicitly renames the extracted directory.

## KRaft Initialization Behavior

`kraft.yml` protects existing Kafka data.

The high-level sequence is:

```text
Determine first Kafka inventory host
             |
             v
Check authoritative cluster ID
             |
             +-- absent --> generate cluster ID
             |
             v
Persist/read cluster ID
             |
             v
Distribute same cluster ID to every node
             |
             v
Check meta.properties
             |
             +-- exists --> verify cluster ID and do NOT format
             |
             v
Generate/read unique directory ID on every controller
             |
             v
Build initial controller list
             |
             v
Format only previously unformatted storage
             |
             v
Verify meta.properties
```

The role must never blindly reformat an existing Kafka data directory.

The protection check verifies that existing metadata belongs to the expected cluster.

### Kafka 4.x formatting

The tested Kafka 4.3.1 format command uses:

```bash
/app/kafka/bin/kafka-storage.sh format \
  --cluster-id <cluster-id> \
  --config /app/kafka/config/kraft/server.properties \
  --initial-controllers \
  '1@10.148.0.3:9093:<directory-id-1>,2@10.148.0.2:9093:<directory-id-2>'
```

Kafka 4.3.1 requires the directory ID after the controller port when using `--initial-controllers`.

Using only:

```text
1@10.148.0.3:9093
```

results in an error similar to:

```text
No colon following port could be found.
```

## Systemd Management

Kafka is managed by an Ansible-generated systemd service.

Typical lifecycle commands:

```bash
systemctl status kafka
systemctl start kafka
systemctl stop kafka
systemctl restart kafka
journalctl -u kafka
```

Ansible:

```bash
ansible kafka -b -m systemd -a "name=kafka state=restarted"
```

Check:

```bash
ansible kafka -b -m shell -a \
'systemctl --no-pager -l status kafka; \
echo "===== JOURNAL ====="; \
journalctl -u kafka -n 100 --no-pager'
```

The service should run as:

```text
User=kafka
Group=kafka
```

rather than root.

## Replication Settings

### Two-node lab

A two-node cluster is useful for testing installation, broker communication, topic replication, and Ansible behavior.

A test topic can use:

```text
replication-factor=2
```

However, two KRaft controllers do not provide useful controller fault tolerance because a majority requires both controllers.

### Three-node target

For three nodes:

```yaml
kafka_default_replication_factor: 3
kafka_min_insync_replicas: 2

kafka_offsets_topic_replication_factor: 3

kafka_transaction_state_log_replication_factor: 3
kafka_transaction_state_log_min_isr: 2
```

This allows one replica/controller node to be unavailable while maintaining a majority, subject to the configured producer acknowledgement and ISR behavior.

## Validation

### Service status

```bash
ansible kafka -b -m shell -a \
'systemctl is-active kafka'
```

Expected:

```text
active
```

### Ports

```bash
ansible kafka -b -m shell -a \
'ss -lntp | grep -E ":9092|:9093"'
```

Expected on each combined broker/controller:

```text
*:9092 LISTEN
*:9093 LISTEN
```

### Broker API

Node 1:

```bash
/app/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.148.0.3:9092
```

Node 2:

```bash
/app/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server 10.148.0.2:9092
```

Healthy brokers report their broker IDs and supported APIs with:

```text
isFenced: false
```

### KRaft metadata

Check:

```bash
cat /app/kafka-data/meta.properties
```

All nodes should have the same cluster ID and their own node-specific identity.

Do not delete or regenerate this metadata on a live cluster.

## Functional Testing

### Create a test topic

Example two-node lab topic:

```bash
/app/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.148.0.3:9092 \
  --create \
  --topic ansible-test \
  --partitions 3 \
  --replication-factor 2
```

### Describe the topic

```bash
/app/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.148.0.3:9092 \
  --describe \
  --topic ansible-test
```

A successful tested result had:

```text
Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2,1
Partition: 2  Leader: 1  Replicas: 1,2  Isr: 1,2
```

This verifies:

- both brokers are registered;
- both brokers participate in replication;
- leaders are distributed;
- both replicas are in-sync.

### Produce test messages

```bash
printf 'message-001\nmessage-002\nmessage-003\nmessage-004\nmessage-005\n' | \
/app/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 10.148.0.3:9092 \
  --topic ansible-test
```

### Consume through another broker

```bash
/app/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 10.148.0.2:9092 \
  --topic ansible-test \
  --from-beginning \
  --max-messages 5
```

This tests application-level broker connectivity across the cluster.

## Idempotency

The role is designed so repeated runs do not reformat existing Kafka storage.

Important persistent files are:

```text
/app/kafka-cluster.id
/app/kafka-directory.id
/app/kafka-data/meta.properties
```

On subsequent executions:

- the existing cluster ID is reused;
- each existing directory ID is reused;
- existing `meta.properties` is detected;
- cluster ownership is verified;
- storage formatting is skipped;
- configuration is updated only when required;
- systemd remains managed by Ansible.

A rerun must never create a new cluster ID for an existing cluster.

## Uninstall Behavior

The uninstall path stops Kafka and removes selected components.

Recommended defaults:

```yaml
kafka_uninstall_remove_installation: true
kafka_uninstall_remove_data: false
kafka_uninstall_remove_logs: true
kafka_uninstall_remove_cluster_metadata: false
kafka_uninstall_remove_user: false
```

This removes the software installation while preserving cluster data and identity.

### Standard uninstall

```bash
ansible-playbook \
  playbooks/uninstall_kafka.yml \
  -v
```

### Full lab reset

For a clean rebuild test:

```bash
ansible-playbook \
  playbooks/uninstall_kafka.yml \
  -e kafka_uninstall_remove_data=true \
  -e kafka_uninstall_remove_cluster_metadata=true \
  -e kafka_uninstall_remove_user=true \
  -v
```

A full reset may remove:

```text
/etc/systemd/system/kafka.service
/app/kafka
/app/kafka_2.13-4.3.1
/app/kafka-data
/app/kafka-logs
/app/kafka-cluster.id
/app/kafka-directory.id
kafka OS user/group
```

The source archive remains in:

```text
roles/kafka/files/kafka_2.13-4.3.1.tgz
```

### Check mode

Post-uninstall validation should be skipped during Ansible check mode because `--check` predicts removals but does not actually remove files.

For example:

```yaml
when:
  - not ansible_check_mode
```

should be applied to runtime validation that expects the service/file/process to have actually disappeared.

## Scaling from Lab to Three Nodes

The automation is intentionally inventory-driven.

To add `kafka-03`:

1. provision the third RHEL 9 node;
2. ensure Java repositories are available;
3. configure SSH access;
4. add the node to `inventory/hosts.yml`;
5. assign a unique `kafka_node_id`;
6. ensure TCP 9093 is allowed between all three nodes;
7. update replication settings for three nodes;
8. deploy the cluster from a clean bootstrap state when creating a new cluster.

Example:

```yaml
kafka-03:
  ansible_host: 10.148.0.4
  kafka_host: 10.148.0.4
  kafka_node_id: 3
```

For a new three-node cluster, the dynamically generated initial-controller string will conceptually become:

```text
1@10.148.0.3:9093:<id1>,2@10.148.0.2:9093:<id2>,3@10.148.0.4:9093:<id3>
```

Do not treat adding a controller to an already running dynamic KRaft cluster as equivalent to rebuilding a fresh cluster. Controller membership changes should follow the Kafka procedure appropriate to the running cluster and version.

## Troubleshooting

### SSH permission denied

Symptom:

```text
Permission denied (publickey,password)
```

Verify direct SSH:

```bash
ssh rhel9@10.148.0.3
```

Then:

```bash
ansible kafka -m ping
```

Verify privilege escalation:

```bash
ansible kafka -b -m command -a "id"
```

### Java package unavailable

Symptom:

```text
No matching Packages to list
```

Check:

```bash
dnf repolist
subscription-manager status
```

Then:

```bash
dnf list java-21-openjdk-headless
```

The node must have a repository providing Java 21.

### Installation directory mismatch

Apache Kafka archive names include the Scala version:

```text
kafka_2.13-4.3.1
```

If Ansible expects:

```text
/app/kafka-4.3.1
```

but extraction creates:

```text
/app/kafka_2.13-4.3.1
```

the validation will fail.

Verify:

```bash
ansible kafka -b -m shell -a \
'ls -ld /app/kafka* 2>/dev/null || true'
```

Align `install.yml` and `kafka_install_dir` with the actual installation layout.

### Ansible temporary directory permission error

A task executed as the `kafka` account may fail with an error involving:

```text
/home/kafka/.ansible/tmp
```

or:

```text
Failed to create temporary directory
```

Avoid switching the entire Ansible task context to a service account with an inaccessible home when not necessary.

For commands that must run as Kafka, a pattern such as:

```bash
runuser -u kafka -- <command>
```

can keep Ansible's remote execution under the privileged account while running only the Kafka command as the service user.

### KRaft format requires initial controllers

Kafka 4.3.1 may report:

```text
Because controller.quorum.voters is not set on this controller,
you must specify one of the following:
--standalone, --initial-controllers, or --no-initial-controllers
```

The role uses dynamic KRaft controller bootstrapping and therefore supplies:

```text
--initial-controllers
```

### Invalid initial-controller format

Symptom:

```text
No colon following port could be found.
```

Incorrect:

```text
1@10.148.0.3:9093,2@10.148.0.2:9093
```

Correct Kafka 4.x form:

```text
1@10.148.0.3:9093:<directory-id-1>,2@10.148.0.2:9093:<directory-id-2>
```

### systemd fails with 203/EXEC

Example:

```text
status=203/EXEC
Permission denied
```

First test the script directly:

```bash
runuser -u kafka -- \
  /app/kafka/bin/kafka-server-start.sh --version
```

Then inspect:

```bash
namei -l /app/kafka/bin/kafka-server-start.sh
ls -lZ /app/kafka/bin/kafka-server-start.sh
findmnt -T /app/kafka/bin/kafka-server-start.sh
getenforce
```

During the tested lab deployment, SELinux denied systemd access to a Kafka symlink labeled `default_t`.

For production, use a persistent SELinux policy/context appropriate for the installation rather than disabling SELinux.

### Controller connection fails

Example:

```text
Connection to node 1 (/10.148.0.3:9093) could not be established.
```

Test both directions:

```bash
timeout 3 bash -c "</dev/tcp/10.148.0.2/9093"
timeout 3 bash -c "</dev/tcp/10.148.0.3/9093"
```

Check:

```bash
systemctl is-active firewalld
firewall-cmd --list-all
```

An asymmetric firewall configuration can allow:

```text
kafka-01 -> kafka-02
```

while blocking:

```text
kafka-02 -> kafka-01
```

KRaft requires controller connectivity in both directions.

### 9093 is listening but 9092 is not

If only the controller listener appears:

```text
*:9093 LISTEN
```

but broker port 9092 is absent, inspect the Kafka journal:

```bash
journalctl -u kafka -n 100 --no-pager
```

The broker may still be waiting for controller registration or quorum stabilization.

Once the KRaft quorum is healthy, the broker listener should become available.

### Kafka service logs

```bash
systemctl status kafka --no-pager -l
journalctl -u kafka -n 100 --no-pager
```

Filter important messages:

```bash
journalctl -u kafka --no-pager | \
grep -Ei "ERROR|FATAL|Exception|quorum|controller|broker|registration|leader" | \
tail -100
```

### Broker API validation

```bash
/app/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server <broker-ip>:9092
```

A healthy broker should report:

```text
isFenced: false
```

### Metadata quorum direct-controller error

A command using:

```bash
kafka-metadata-quorum.sh --bootstrap-controller ...
```

may return an `UnsupportedVersionException` if the current metadata version does not support the required direct-to-controller Admin API.

This does not by itself prove the Kafka cluster is unhealthy. Validate broker registration, service logs, listeners, topic creation, replicas, and ISR as well.

## Security and Production Recommendations

The tested lab intentionally relaxed some host security controls while isolating deployment issues. Production should be hardened.

### SELinux

Do not make disabling SELinux the production standard.

Instead:

- define persistent SELinux file contexts for Kafka under `/app`;
- validate systemd can execute Kafka binaries;
- keep SELinux enforcing where required by the platform security standard.

### Firewall

Do not simply disable `firewalld` in production.

Permit:

```text
9092/tcp
9093/tcp
```

according to the network design.

Restrict `9093/tcp` to Kafka controller nodes.

Restrict `9092/tcp` to authorized applications, load balancers, administration hosts, and Kafka nodes as required.

### Kafka security

The current lab validates an unencrypted internal deployment.

Production should consider:

- TLS for client/broker communication;
- TLS for inter-broker/controller traffic where required;
- SASL authentication;
- Kafka ACLs;
- least-privilege application identities;
- secure secret management;
- certificate lifecycle automation.

### Ansible

- Use SSH keys rather than passwords where possible.
- Use Ansible Vault for credentials and secrets.
- Do not commit private keys.
- Keep Kafka installation checksums under version control.
- Validate the Apache Kafka archive before deployment.
- Restrict repository access.
- Test upgrades in a non-production cluster first.

### Data protection

Before destructive uninstall or cluster rebuild:

- back up required Kafka data;
- understand topic replication and retention;
- preserve cluster identity when performing a reinstall;
- never delete `meta.properties` casually;
- do not reformat a live data directory.

## Tested Lifecycle

The current Kafka 4.3.1 lab has been exercised through:

```text
RHEL 9 nodes
     |
     v
Ansible SSH validation
     |
     v
Java 21 prerequisites
     |
     v
Kafka 4.3.1 installation
     |
     v
server.properties generation
     |
     v
Persistent cluster ID
     |
     v
Unique controller directory IDs
     |
     v
KRaft storage formatting
     |
     v
Ansible-managed systemd
     |
     v
Controller listener :9093
     |
     v
Broker listener :9092
     |
     v
Broker API validation
     |
     v
Create ansible-test topic
     |
     v
3 partitions / replication factor 2
     |
     v
Both brokers present in ISR
```

The tested two-node topic state demonstrated:

```text
Partition 0 -> Leader 1 -> Replicas 1,2 -> ISR 1,2
Partition 1 -> Leader 2 -> Replicas 2,1 -> ISR 2,1
Partition 2 -> Leader 1 -> Replicas 1,2 -> ISR 1,2
```

This verifies that the automation can produce a functioning multi-node Kafka KRaft deployment.

The intended next production-oriented topology is:

```text
3 Kafka nodes
3 brokers
3 controllers
replication factor 3
min.insync.replicas 2
2-of-3 KRaft controller majority
```

This repository should be treated as the automation authority for Kafka installation, KRaft initialization, configuration, systemd lifecycle, validation, and controlled uninstall.

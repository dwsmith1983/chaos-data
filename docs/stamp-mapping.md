# STAMP Loss Scenario Mapping

This document maps chaos categories to STAMP (System-Theoretic Accident Model and Processes) loss scenarios. Each category of chaos is analyzed for its impact on the control loop, specifically how it contributes to inadequate control, timing issues, incorrect actions, or omissions.

## Mapping Overview

| Chaos Category | STAMP Loss Scenario | Description of Impact |
| :--- | :--- | :--- |
| **Temporal** | Control action too late | Delays in processing, ingestion, or signaling that cause control actions to occur outside the safety window. |
| **Numeric** | Incorrect control action | Forged or corrupted numerical values (timestamps, counts) leading the controller to make decisions based on false metrics. |
| **Encoding** | Inadequate control action | Data corruption or incompatible encoding that prevents the controller from correctly interpreting feedback. |
| **Schema-drift** | Inadequate control action | Unexpected changes in data structure that lead to partial processing or failure to extract critical control signals. |
| **Nulls** | Inadequate control action | Missing values in required fields that cause the controller to operate on incomplete state information. |
| **Structural** | Missing control action | Dropped records or split events that result in the controller failing to recognize a state change that requires action. |
| **Protocol** | Incorrect control action | False success signals or replayed events that trick the controller into acting on stale or non-existent states. |
| **Concurrency** | Incorrect control action | Race conditions in state updates that lead to non-deterministic or inconsistent control decisions. |
| **Volume** | Control action too late | Resource exhaustion under high volume that degrades system responsiveness. |
| **Referential** | Inadequate control action | Broken links between data entities (e.g., missing parent IDs) that prevent the controller from correlating related events. |
| **Injection** | Incorrect control action | Malicious or malformed inputs that bypass validation to execute unintended control commands. |
| **Go-specific** | Missing control action | Runtime failures such as panics, deadlock, or goroutine leaks that halt the controller entirely. |

## Detailed Scenario Analysis

### 1. Inadequate Control Action
*The control action is provided but does not produce the desired effect.*
- **Categories**: Encoding, Schema-drift, Nulls, Referential.
- **Example**: A `schema-drift` mutation removes a field used for routing, causing the data to be processed by a default handler that doesn't satisfy safety requirements.

### 2. Control Action Too Late
*The control action is provided but outside the required time window.*
- **Categories**: Temporal, Volume.
- **Example**: `cascade-delay` in a pipeline causes a watchdog timer to expire before the "all-clear" signal can be issued, triggering an unnecessary emergency shutdown.

### 3. Incorrect Control Action
*The control action is provided but it is wrong.*
- **Categories**: Numeric, Protocol, Concurrency, Injection.
- **Example**: `timestamp-forgery` makes a stale data packet look recent, causing the controller to overwrite newer state with older data.

### 4. Missing Control Action
*The control action was not provided when it should have been.*
- **Categories**: Structural, Go-specific.
- **Example**: `drop` mutation deletes the specific trigger record required to initiate a cleanup task, leading to resource leakage.

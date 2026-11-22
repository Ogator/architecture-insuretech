# Решение на основе Event-Driven Architecture

## Обзор изменений

Текущая архитектура основана на синхронных REST API и периодическом polling. Это приводит к проблемам с latency, stale data, tight coupling и cascade failure при росте числа страховых компаний.

Предлагаемое решение переводит взаимодействие между ключевыми сервисами на асинхронную event-driven архитектуру с использованием Apache Kafka как event broker.

---

## Архитектурные решения

### 1. Введение Apache Kafka как Event Bus

**Решение:**
В центр архитектуры добавлен Apache Kafka — distributed event streaming platform.

**Топики:**
- `insurance.product.updated` — обновление данных о продуктах и тарифах
- `insurance.policy.created` — создание новой страховки

**Обоснование:**
- **Decoupling:** Сервисы не знают друг о друге, только о структуре событий
- **Scalability:** Новые consumer'ы подписываются на топики без изменения producer'ов
- **Fault tolerance:** Kafka хранит события в логе, consumer может перечитать их при восстановлении
- **Backpressure handling:** Consumer обрабатывает события в своём темпе

---

### 2. Переход на push-модель для данных о продуктах

#### До:
- `core-app` polling `ins-product-aggregator` каждые 15 минут
- `ins-comp-settlement` polling `ins-product-aggregator` раз в сутки
- Данные устаревают между опросами

#### После:
- `ins-product-aggregator` периодически (раз в час) опрашивает страховые компании
- При обнаружении изменений публикует событие `insurance.product.updated` в Kafka
- `core-app` и `ins-comp-settlement` подписаны на этот топик
- Consumer'ы получают события в real-time и обновляют локальные реплики

**Преимущества:**
- **Real-time updates:** staleness данных снижается с 15 минут до секунд
- **Снижение нагрузки:** aggregator опрашивается один раз, результат доставляется N consumer'ам через fan-out
- **Нет лишних запросов:** событие публикуется только при изменении данных

---

### 3. Переход на event streaming для данных о страховках

#### До:
- `ins-comp-settlement` раз в сутки запрашивает у `core-app` список оформленных страховок
- Batch обработка с задержкой до 24 часов

#### После:
- `core-app` публикует событие `insurance.policy.created` в Kafka при оформлении каждой страховки
- `ins-comp-settlement` подписан на этот топик
- Consumer обрабатывает события по мере поступления

**Преимущества:**
- **Real-time settlement:** взаиморасчёты формируются в момент оформления страховки
- **Stream processing вместо batch:** нет ночного пика нагрузки
- **Упрощение интеграции:** другие сервисы (аналитика, уведомления) могут подписаться на тот же топик

---

### 4. Асинхронная агрегация с partial failure handling

#### До:
- `ins-product-aggregator` синхронно запрашивает 5 компаний, блокируется на самой медленной
- При timeout одной компании весь запрос падает

#### После:
- Aggregator запрашивает все компании параллельно с timeout (например, 5 секунд)
- Если компания не ответила — пропускаем её в текущем цикле, логируем ошибку
- Публикуется событие с данными от доступных компаний
- Consumer'ы видят частичный результат и сами решают, как с этим работать

**Преимущества:**
- **Graceful degradation:** система работает даже при частичной недоступности
- **Сокращение latency:** не ждём медленную компанию
- **Observability:** легко видеть, какая компания проблемная

---

### 5. Использование Transactional Outbox Pattern

#### Проблема:
Атомарность между изменением данных в БД и публикацией события в Kafka. Если приложение упадёт между `INSERT` в БД и `kafka.send()`, событие потеряется.

#### Решение:
Используем паттерн **Transactional Outbox**.

**Как работает:**

1. **core-app:**
   - При оформлении страховки выполняется транзакция:
     ```sql
     BEGIN;
     INSERT INTO policies (...) VALUES (...);
     INSERT INTO outbox (event_type, aggregate_id, payload, created_at)
       VALUES ('insurance.policy.created', policy_id, '{"policyId": ...}', NOW());
     COMMIT;
     ```
   - Обе записи (бизнес-данные + событие) сохраняются атомарно в одной транзакции

2. **Outbox Relay (CDC или polling):**
   - Отдельный процесс читает события из `outbox` таблицы
   - Публикует события в Kafka
   - Помечает опубликованные события как `published = true` или удаляет их

3. **Consumer (ins-comp-settlement):**
   - Читает событие из Kafka
   - Обрабатывает его идемпотентно (проверяет `policy_id`, чтобы не обработать дважды)

**Альтернативы:**
- **Change Data Capture (CDC):** Debezium читает WAL PostgreSQL и публикует изменения в Kafka
- **Polling Outbox:** Scheduled job в самом приложении периодически читает `outbox` и публикует события

**Выбор: Polling Outbox**

Для MVP используем polling outbox (Spring `@Scheduled`), так как:
- Проще настроить (не требует Debezium/Kafka Connect)
- Меньше инфраструктурных зависимостей
- Latency в пределах секунды приемлема для бизнес-кейса

При росте нагрузки можно мигрировать на CDC.

**Обоснование использования Transactional Outbox:**
- **At-least-once delivery:** событие гарантированно будет опубликовано (consumer должен быть идемпотентен)
- **Consistency:** нет race condition между БД и Kafka
- **Durability:** при падении приложения события не теряются

---

### 6. Декомпозиция функциональности остаётся без изменений

**Требование задания:**
> Не меняя декомпозицию функциональности между сервисами...

**Выполнено:**
- `core-app` по-прежнему отвечает за оформление страховок
- `ins-product-aggregator` по-прежнему агрегирует данные от страховых компаний
- `ins-comp-settlement` по-прежнему ведёт взаиморасчёты
- `client-info` по-прежнему хранит данные клиентов

**Изменились только способы коммуникации:** синхронный REST → асинхронные события через Kafka.

---

## Сравнение: До и После

| Аспект | До (REST + Polling) | После (Event-Driven + Outbox) |
|--------|---------------------|-------------------------------|
| **Latency агрегации** | max(latency₁, ..., latencyₙ) | Асинхронно, не блокирует клиентов |
| **Staleness данных** | До 15 минут | Секунды (real-time push) |
| **Доступность** | 90% (при 10 компаниях, p=0.99) | 99% (partial failure) |
| **Coupling** | Tight (REST sync) | Loose (async events) |
| **Масштабируемость** | O(N×M) запросов (N клиентов, M компаний) | O(M) запросов + fan-out через Kafka |
| **Нагрузка на aggregator** | 4 req/hour × N клиентов | 1 запрос/час к страховым, 1 событие в Kafka |
| **Взаиморасчёты** | Batch (раз в сутки) | Stream (real-time) |
| **Fault tolerance** | Cascade failure | Buffering в Kafka, graceful degradation |
| **Consistency** | No guarantees | Transactional Outbox → at-least-once |

---

## Новые взаимодействия

### 1. core-app → Kafka
- **Publish:** `insurance.policy.created`
- **Subscribe:** `insurance.product.updated`

### 2. ins-product-aggregator → Kafka
- **Publish:** `insurance.product.updated`

### 3. ins-comp-settlement → Kafka
- **Subscribe:** `insurance.policy.created`, `insurance.product.updated`

### 4. Удалённые взаимодействия
- ~~`core-app` → `ins-product-aggregator` (REST polling каждые 15 минут)~~
- ~~`ins-comp-settlement` → `ins-product-aggregator` (REST polling раз в сутки)~~
- ~~`ins-comp-settlement` → `core-app` (REST batch request раз в сутки)~~

---

## Структура события

### insurance.policy.created

```json
{
  "eventId": "uuid-v4",
  "eventType": "insurance.policy.created",
  "timestamp": "2026-11-22T10:30:00Z",
  "aggregateId": "policy-12345",
  "payload": {
    "policyId": "policy-12345",
    "clientId": "client-67890",
    "insuranceCompanyId": "company-001",
    "productId": "product-456",
    "premium": 15000,
    "startDate": "2026-12-01",
    "endDate": "2027-12-01",
    "status": "ACTIVE"
  }
}
```

### insurance.product.updated

```json
{
  "eventId": "uuid-v4",
  "eventType": "insurance.product.updated",
  "timestamp": "2026-11-22T10:00:00Z",
  "aggregateId": "product-456",
  "payload": {
    "products": [
      {
        "productId": "product-456",
        "insuranceCompanyId": "company-001",
        "name": "ОСАГО Стандарт",
        "premium": 15000,
        "coverage": 500000,
        "updatedAt": "2026-11-22T09:55:00Z"
      },
      ...
    ],
    "unavailableCompanies": ["company-003"]
  }
}
```

---

## Требования к инфраструктуре

### Apache Kafka
- **Deployment:** 3-node Kafka cluster (для HA)
- **Replication factor:** 3
- **Min in-sync replicas:** 2
- **Retention:** 7 дней (для replay и восстановления consumer'ов)

### Transactional Outbox
- **Outbox таблица в core-db:**
  ```sql
  CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published BOOLEAN NOT NULL DEFAULT FALSE,
    published_at TIMESTAMP
  );
  CREATE INDEX idx_outbox_published ON outbox(published, created_at);
  ```

- **Outbox Relay:** Spring `@Scheduled` job каждые 1-5 секунд

### Idempotency в consumer'ах
- Consumer должен проверять, не обработано ли событие с данным `aggregateId`
- Например, таблица `processed_events`:
  ```sql
  CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
  );
  ```

---

## Риски и митигация

### 1. Дубликаты событий (at-least-once delivery)
**Риск:** Outbox relay может опубликовать событие дважды (например, при сбое между публикацией и пометкой `published = true`).

**Митигация:**
- Consumer обязан быть идемпотентен
- Использовать `eventId` или `aggregateId` для дедупликации

### 2. Ordering гарантии
**Риск:** Kafka гарантирует порядок только внутри одной партиции.

**Митигация:**
- Использовать `aggregateId` как partition key (все события по одному policy попадут в одну партицию)
- Consumer обрабатывает события в порядке получения

### 3. Latency polling outbox
**Риск:** Polling outbox каждые 5 секунд добавляет latency до 5 секунд.

**Митигация:**
- Уменьшить интервал до 1 секунды
- При необходимости мигрировать на CDC (Debezium), который публикует события в миллисекундах

### 4. Schema evolution
**Риск:** Изменение структуры событий ломает старых consumer'ов.

**Митигация:**
- Использовать Schema Registry (Confluent Schema Registry или аналог)
- Versioning событий (добавлять поле `version: "1.0"`)
- Backward/forward compatibility при изменении схемы

### 5. Dead Letter Queue (DLQ)
**Риск:** Consumer не может обработать событие (validation failed, bug в коде).

**Митигация:**
- Конфигурировать DLQ топик для failed events
- Настроить alerting на рост DLQ
- Ручная обработка или re-publish после исправления

---

## Переходный план (миграция)

### Этап 1: Подготовка инфраструктуры
1. Развернуть Kafka cluster (3 nodes)
2. Создать топики `insurance.policy.created`, `insurance.product.updated`
3. Добавить `outbox` таблицу в `core-db`

### Этап 2: Dual writes (переходный режим)
1. `core-app` начинает писать в outbox + публиковать события
2. `ins-comp-settlement` продолжает polling `core-app` (старый путь)
3. Параллельно `ins-comp-settlement` подписывается на Kafka и логирует события (без обработки)
4. Мониторим, что события доставляются корректно

### Этап 3: Переключение consumer'ов
1. `ins-comp-settlement` переключается на чтение из Kafka (новый путь)
2. Отключаем старый polling
3. Monitoring: сравниваем результаты обработки (старый vs новый путь)

### Этап 4: Аналогично для `ins-product-aggregator`
1. Aggregator публикует `insurance.product.updated`
2. `core-app` и `ins-comp-settlement` подписываются
3. Отключаем polling aggregator'а

### Этап 5: Удаление legacy кода
1. Удалить REST endpoints для batch запросов
2. Удалить polling job'ы
3. Cleanup кода

---

## Метрики для мониторинга

1. **Kafka lag:** разница между последним offset в топике и текущим offset consumer'а
2. **Outbox size:** количество неопубликованных событий в `outbox` таблице
3. **Event processing time:** от `created_at` в outbox до обработки consumer'ом
4. **DLQ size:** количество failed events
5. **Aggregator availability:** доля страховых компаний, ответивших успешно
6. **Event publish rate:** количество событий/секунду

---

## Итоговая архитектура

[Event-Driven Architecture](InsureTech_C4_container-diagram_event-driven.drawio.xml)

**Основные изменения:**
- Добавлен Apache Kafka в центр
- `core-app` публикует `insurance.policy.created`, подписан на `insurance.product.updated`
- `ins-product-aggregator` публикует `insurance.product.updated`
- `ins-comp-settlement` подписан на оба топика
- Transactional Outbox в `core-db`
- Удалены синхронные polling REST calls между `core-app` ↔ `aggregator` и `settlement` ↔ `core-app`

**Сохранены без изменений:**
- REST API для клиентских запросов (Web/Partners → core-app)
- REST API для взаимодействия с внешними системами (страховые компании, платёжный сервис)
- Разделение ответственности между сервисами

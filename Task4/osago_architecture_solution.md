# Задание 4. Проектирование продажи ОСАГО — Архитектурное решение

## Обзор задачи

**Бизнес-требования:**
- Real-time отображение предложений от страховых компаний (как только пришёл ответ)
- Максимальное время ожидания ответа от страховой: **60 секунд**
- Пиковая нагрузка: **2500 одновременных пользователей**, создающих заявку на ОСАГО
- Все страховые компании предоставляют однотипные REST API:
  1. `POST /osago/application` — создать заявку на ОСАГО
  2. `GET /osago/application/{id}` — получить предложение по заявке

**Архитектурные решения команды:**
1. Использовать подход, как для получения данных о продуктах (Event-Driven из задания 3)
2. Выделить отдельный сервис **osago-aggregator** для взаимодействия со страховыми

---

## Архитектурные решения

### 1. Хранилище для osago-aggregator

**Решение: Redis (in-memory cache с TTL)**

**Зачем нужно:**
- Кэширование заявок и ответов от страховых компаний (TTL 60-120 секунд)
- Дедупликация запросов (idempotency key)
- Координация между репликами osago-aggregator
- Pub/Sub для broadcast событий внутри кластера реплик

**Схема данных:**

```redis
# Заявка клиента
osago:application:{client_id}:{uuid}
TTL: 120 секунд
Payload: {
  "clientId": "client-123",
  "carModel": "Toyota Camry",
  "year": 2020,
  "region": "77",
  "driverExperience": 10,
  "createdAt": "2026-11-22T12:00:00Z"
}

# Ответ от страховой компании
osago:offer:{application_id}:{insurance_company_id}
TTL: 120 секунд
Payload: {
  "insuranceCompanyId": "company-001",
  "premium": 15000,
  "coverage": 500000,
  "conditions": "...",
  "status": "READY|PROCESSING|TIMEOUT|ERROR",
  "receivedAt": "2026-11-22T12:00:05Z"
}

# Idempotency key для retry
osago:idempotency:{application_id}:{company_id}
TTL: 60 секунд
Value: "request_sent"
```

**Обоснование:**
- **Redis вместо PostgreSQL**: данные временные (TTL), высокая пропускная способность (2500 RPS)
- **TTL 120 сек**: больше максимального timeout (60 сек), чтобы клиент мог переподключиться
- **Pub/Sub**: при получении ответа от страховой aggregator публикует в Redis Pub/Sub, другие реплики получают событие

---

### 2. API для Web-приложения

**Требование:** клиент должен видеть предложения **сразу**, как только они приходят от страховых.

**Решение: Server-Sent Events (SSE)**

**Почему SSE, а не WebSocket:**
- **One-way streaming**: клиент только получает данные, не отправляет (кроме initial request)
- **Проще**: SSE — это HTTP, не требует upgrade to WebSocket protocol
- **Автоматический reconnect**: браузер переподключается при обрыве
- **Firewall-friendly**: работает через стандартный HTTP/HTTPS

**API:**

```http
POST /api/osago/applications
Content-Type: application/json

{
  "clientId": "client-123",
  "carModel": "Toyota Camry",
  "year": 2020,
  "region": "77",
  "driverExperience": 10
}

Response: 201 Created
Location: /api/osago/applications/{application_id}
{
  "applicationId": "app-uuid-123",
  "streamUrl": "/api/osago/applications/app-uuid-123/offers/stream"
}
```

```http
GET /api/osago/applications/{application_id}/offers/stream
Accept: text/event-stream

Response: 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

event: offer
id: 1
data: {"insuranceCompanyId": "company-001", "premium": 15000, "status": "READY"}

event: offer
id: 2
data: {"insuranceCompanyId": "company-002", "premium": 14500, "status": "READY"}

event: timeout
id: 3
data: {"insuranceCompanyId": "company-003", "status": "TIMEOUT"}

event: complete
id: 4
data: {"totalOffers": 8, "timeouts": 2, "errors": 0}
```

**Последовательность:**
1. Client → POST `/api/osago/applications` → `core-app`
2. Client получает `streamUrl`
3. Client → GET `streamUrl` (SSE connection) → `core-app`
4. `core-app` держит SSE соединение открытым
5. По мере поступления предложений от `osago-aggregator`, `core-app` отправляет `event: offer` в SSE stream
6. Через 60 секунд или когда все компании ответили → `event: complete`, SSE закрывается

---

### 3. API между core-app и osago-aggregator

**Решение: gRPC с Server Streaming**

**Почему gRPC:**
- **Эффективнее REST**: binary protocol (Protobuf), меньше overhead
- **Type-safe**: схема в `.proto` файле, code generation
- **Server Streaming**: aggregator стримит предложения core-app по мере готовности
- **Backpressure**: встроенная поддержка flow control
- **Multiplexing**: HTTP/2, несколько стримов на одно TCP соединение

**Protobuf схема:**

```protobuf
syntax = "proto3";

package osago;

service OsagoAggregatorService {
  // Server streaming: aggregator отправляет предложения по мере готовности
  rpc StreamOffers(StreamOffersRequest) returns (stream OfferResponse);
}

message StreamOffersRequest {
  string application_id = 1;
  string client_id = 2;
  string car_model = 3;
  int32 year = 4;
  string region = 5;
  int32 driver_experience = 6;
  repeated string insurance_company_ids = 7; // список компаний для опроса
}

message OfferResponse {
  string insurance_company_id = 1;
  
  oneof result {
    Offer offer = 2;
    OfferError error = 3;
  }
  
  OfferStatus status = 4;
  int64 latency_ms = 5; // latency запроса к страховой
}

message Offer {
  double premium = 1;
  double coverage = 2;
  string conditions = 3;
  string policy_url = 4;
}

message OfferError {
  ErrorCode code = 1;
  string message = 2;
}

enum OfferStatus {
  READY = 0;
  TIMEOUT = 1;
  ERROR = 2;
}

enum ErrorCode {
  UNKNOWN = 0;
  TIMEOUT_EXCEEDED = 1;
  INSURANCE_API_ERROR = 2;
  VALIDATION_ERROR = 3;
  CIRCUIT_BREAKER_OPEN = 4;
}
```

**Последовательность:**

```
core-app                        osago-aggregator                  Insurance Company 1/2/3
   |                                     |                                  |
   |--StreamOffersRequest(app-123)----->|                                  |
   |                                     |---POST /osago/application------->| (асинхронно)
   |                                     |---POST /osago/application------->| (асинхронно)
   |                                     |---POST /osago/application------->| (асинхронно)
   |                                     |                                  |
   |                                     |<-----200 OK {offerId: 001}-------| (5 сек)
   |<---OfferResponse(company-001)------|                                  |
   |                                     |                                  |
   |                                     |<-----200 OK {offerId: 002}-------| (8 сек)
   |<---OfferResponse(company-002)------|                                  |
   |                                     |                                  |
   |                                     |      (timeout 60 сек)            X (company-003)
   |<---OfferResponse(TIMEOUT)----------|                                  |
   |                                     |                                  |
   |<---stream closed--------------------|                                  |
```

**Преимущества:**
- core-app получает предложения **по мере готовности**, не блокируясь
- osago-aggregator может отправить `TIMEOUT` или `ERROR` для конкретной компании
- Backpressure: если core-app не успевает обрабатывать, gRPC замедлит отправку

---

### 4. Взаимодействие osago-aggregator со страховыми компаниями

**Проблема:**
- Страховые предоставляют REST API: `POST /osago/application` + `GET /osago/application/{id}`
- Нужно опросить 10 компаний **параллельно** с timeout 60 секунд

**Решение: Асинхронные HTTP-клиенты + Kotlin Coroutines**

**Алгоритм:**

```kotlin
suspend fun streamOffers(request: StreamOffersRequest): Flow<OfferResponse> = flow {
    val companies = request.insuranceCompanyIds
    
    // Запускаем параллельные запросы к страховым компаниям
    coroutineScope {
        companies.map { companyId ->
            async {
                try {
                    // Circuit Breaker + Retry + Timeout
                    val offer = withTimeout(60.seconds) {
                        retryWithBackoff(maxRetries = 2) {
                            circuitBreaker.execute {
                                insuranceApiClient.createAndPollApplication(companyId, request)
                            }
                        }
                    }
                    
                    OfferResponse(
                        insuranceCompanyId = companyId,
                        offer = offer,
                        status = OfferStatus.READY
                    )
                } catch (e: TimeoutCancellationException) {
                    OfferResponse(
                        insuranceCompanyId = companyId,
                        error = OfferError(ErrorCode.TIMEOUT_EXCEEDED, "60s timeout"),
                        status = OfferStatus.TIMEOUT
                    )
                } catch (e: CircuitBreakerOpenException) {
                    OfferResponse(
                        insuranceCompanyId = companyId,
                        error = OfferError(ErrorCode.CIRCUIT_BREAKER_OPEN, e.message),
                        status = OfferStatus.ERROR
                    )
                }
            }
        }.asFlow() // Flow<Deferred<OfferResponse>>
         .flatMapMerge { it.await() } // Отправляем результаты по мере готовности
         .collect { emit(it) }
    }
}

suspend fun createAndPollApplication(companyId: String, request: StreamOffersRequest): Offer {
    // 1. POST /osago/application
    val applicationId = httpClient.post("https://$companyId/osago/application") {
        contentType(ContentType.Application.Json)
        setBody(request.toInsuranceApplicationRequest())
    }.body<InsuranceApplicationResponse>().id
    
    // 2. Polling GET /osago/application/{id} каждые 2 секунды
    while (true) {
        val response = httpClient.get("https://$companyId/osago/application/$applicationId")
            .body<InsuranceOfferResponse>()
        
        when (response.status) {
            "READY" -> return response.offer
            "PROCESSING" -> delay(2.seconds) // exponential backoff можно добавить
            "ERROR" -> throw InsuranceApiException(response.error)
        }
    }
}
```

---

### 5. Паттерны отказоустойчивости

#### 5.1. Circuit Breaker

**Где:** `osago-aggregator` → страховые компании

**Обоснование:**
- Если страховая компания недоступна (503, timeout), не нужно её бомбардировать запросами
- Circuit Breaker переводит компанию в состояние "OPEN" после N неудачных попыток
- Запросы к этой компании сразу возвращают `ERROR` (fail-fast)
- Через T секунд пробуем снова (Half-Open state)

**Конфигурация (Resilience4j):**

```yaml
resilience4j:
  circuitbreaker:
    configs:
      default:
        slidingWindowSize: 10           # Окно из последних 10 запросов
        failureRateThreshold: 50        # Открыть CB при 50% ошибок
        waitDurationInOpenState: 30s    # Держать CB открытым 30 секунд
        permittedNumberOfCallsInHalfOpenState: 3
    instances:
      insuranceApi:
        baseConfig: default
```

**Обозначение на диаграмме:** иконка `c_role` (Circuit Breaker) на стрелке `osago-aggregator → Insurance Companies`

---

#### 5.2. Timeout

**Где:**
1. `osago-aggregator` → страховые компании: **60 секунд**
2. `core-app` → `osago-aggregator` gRPC: **65 секунд** (больше чем у aggregator)
3. Web → `core-app` SSE: **70 секунд** (больше чем у gRPC)

**Обоснование:**
- Цепочка timeout'ов: каждый уровень даёт +5 секунд на обработку
- Если aggregator не получил ответ за 60 сек, он возвращает `TIMEOUT`
- core-app может закрыть SSE поток через 70 секунд

**Конфигурация:**

```kotlin
// osago-aggregator
withTimeout(60.seconds) {
    insuranceApiClient.createAndPollApplication(companyId, request)
}

// core-app
val grpcStub = stub.withDeadlineAfter(65, TimeUnit.SECONDS)
grpcStub.streamOffers(request).collect { ... }
```

**Обозначение на диаграмме:** иконка `quota` (Timeout) на стрелках:
- `osago-aggregator → Insurance Companies`
- `core-app → osago-aggregator`

---

#### 5.3. Retry

**Где:** `osago-aggregator` → страховые компании

**Обоснование:**
- Transient failures (503, connection reset) — retry
- Permanent failures (400, 422) — не retry
- Максимум 2 retry с exponential backoff

**Конфигурация (Resilience4j):**

```yaml
resilience4j:
  retry:
    configs:
      default:
        maxAttempts: 3                 # Всего 3 попытки (1 основная + 2 retry)
        waitDuration: 1s               # Начальная задержка 1 секунда
        exponentialBackoffMultiplier: 2 # Экспоненциальный рост (1s, 2s, 4s)
        retryExceptions:
          - java.net.ConnectException
          - java.net.SocketTimeoutException
          - org.springframework.web.client.HttpServerErrorException$ServiceUnavailable
    instances:
      insuranceApi:
        baseConfig: default
```

**Обозначение на диаграмме:** иконка `deploy` (Retry) на стрелке `osago-aggregator → Insurance Companies`

---

#### 5.4. Rate Limiting

**Где:** Web → `core-app`

**Обоснование:**
- Защита от DDoS
- Пиковая нагрузка: 2500 одновременных пользователей
- Rate limit: **50 запросов/секунду на IP** + **2500 одновременных SSE соединений глобально**

**Конфигурация (Bucket4j):**

```kotlin
// Per-IP rate limiting
val bucket = Bucket4j.builder()
    .addLimit(Bandwidth.simple(50, Duration.ofSeconds(1))) // 50 req/sec per IP
    .build()

// Global concurrent SSE connections
val sseConnectionSemaphore = Semaphore(2500)

@GetMapping("/api/osago/applications/{id}/offers/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
suspend fun streamOffers(@PathVariable id: String): Flow<ServerSentEvent<OfferResponse>> {
    if (!sseConnectionSemaphore.tryAcquire()) {
        throw TooManyRequestsException("Max concurrent SSE connections reached")
    }
    
    try {
        return flow {
            // ... stream offers
        }
    } finally {
        sseConnectionSemaphore.release()
    }
}
```

**Альтернатива:** Nginx ingress с rate limiting

```nginx
limit_req_zone $binary_remote_addr zone=osago_api:10m rate=50r/s;
limit_conn_zone $binary_remote_addr zone=osago_sse:10m;

location /api/osago/ {
    limit_req zone=osago_api burst=20 nodelay;
    limit_conn osago_sse 10; # max 10 concurrent connections per IP
}
```

**Обозначение на диаграмме:** иконка `job` (Rate Limiter) на стрелке `Web → core-app`

---

### 6. Учёт множественных реплик

**Проблема:**
- `osago-aggregator` и `core-app` задеплоены в нескольких экземплярах
- SSE соединение клиента может попасть на replica-1, а ответы от aggregator прийти в replica-2

**Решение 1: Session Affinity (Sticky Sessions)**

**На уровне ALB/Ingress:**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: core-app
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    service.beta.kubernetes.io/aws-load-balancer-sticky-sessions: "true"
    service.beta.kubernetes.io/aws-load-balancer-cookie-duration: "300" # 5 минут
spec:
  type: LoadBalancer
  sessionAffinity: ClientIP # Sticky by IP
  sessionAffinityConfig:
    clientIP:
      timeoutSeconds: 300
  ports:
    - port: 80
      targetPort: 8080
  selector:
    app: core-app
```

**Обоснование:**
- SSE соединение клиента всегда попадает на тот же pod core-app
- Этот pod держит gRPC stream с osago-aggregator
- Все предложения приходят в этот же pod и отправляются в SSE

**Решение 2: Redis Pub/Sub (если Session Affinity недоступен)**

```kotlin
// core-app replica-1 подписывается на Redis channel
redisTemplate.listenToPattern("osago:offers:${applicationId}") { message ->
    val offer = Json.decodeFromString<OfferResponse>(message)
    sseEmitter.send(SseEmitter.event().data(offer))
}

// osago-aggregator публикует в Redis channel
redisTemplate.convertAndSend("osago:offers:${applicationId}", offer.toJson())
```

**Обоснование:**
- Любая реплика core-app может подписаться на канал `osago:offers:{applicationId}`
- osago-aggregator публикует предложения в Redis Pub/Sub
- Все заинтересованные реплики core-app получают событие
- Та реплика, которая держит SSE соединение с клиентом, отправит событие в stream

**Выбор:** Session Affinity (проще, меньше overhead)

---

## Итоговая архитектура

### Последовательность взаимодействия:

```
Client (Browser)          Web (React)          core-app               osago-aggregator        Redis          Insurance Companies (10)
      |                        |                      |                        |                  |                    |
      |--Fill form------------>|                      |                        |                  |                    |
      |                        |--POST /applications->|                        |                  |                    |
      |                        |                      |---gRPC StreamOffers--->|                  |                    |
      |                        |                      |                        |--Cache app------>|                    |
      |                        |                      |                        |                  |                    |
      |                        |                      |                        |--POST /osago---->|-->(company-001)    |
      |                        |                      |                        |--POST /osago---->|-->(company-002)    |
      |                        |                      |                        |     ...          |-->(company-00N)    |
      |<---streamUrl-----------|<---201 Created-------|                        |                  |                    |
      |                        |                      |                        |                  |                    |
      |--GET streamUrl-------->|                      |                        |                  |                    |
      |    (SSE open)          |                      |                        |                  |                    |
      |                        |                      |                        |<--200 OK offer---|<--(company-001) 5s |
      |                        |                      |<---OfferResponse-------|--Cache offer---->|                    |
      |<---event: offer--------|<---SSE event---------|                        |                  |                    |
      |                        |                      |                        |                  |                    |
      |                        |                      |                        |<--200 OK offer---|<--(company-002) 8s |
      |<---event: offer--------|<---SSE event---------|<---OfferResponse-------|                  |                    |
      |                        |                      |                        |                  |                    |
      |                        |                      |                        |    (timeout 60s) X--(company-003)      |
      |<---event: timeout------|<---SSE event---------|<---OfferResponse-------|                  |                    |
      |                        |                      |                        |                  |                    |
      |<---event: complete-----|<---SSE closed--------|<---gRPC stream closed--|                  |                    |
```

---

## Ключевые решения

| Аспект | Решение | Обоснование |
|--------|---------|-------------|
| **Хранилище osago-aggregator** | Redis (in-memory, TTL 120s) | Временные данные, высокая пропускная способность, Pub/Sub |
| **Web ↔ core-app** | Server-Sent Events (SSE) | One-way streaming, auto-reconnect, firewall-friendly |
| **core-app ↔ osago-aggregator** | gRPC Server Streaming | Binary protocol, type-safe, backpressure, multiplexing |
| **osago-aggregator ↔ Insurance** | REST (async polling) | Заданный API страховых компаний |
| **Circuit Breaker** | osago-aggregator → Insurance | Fail-fast при недоступности страховой |
| **Timeout** | 60s (Insurance), 65s (gRPC), 70s (SSE) | Цепочка timeout'ов с запасом |
| **Retry** | 3 попытки, exponential backoff | Transient failures |
| **Rate Limiting** | 50 req/s per IP, 2500 concurrent SSE | DDoS защита, соответствие пиковой нагрузке |
| **Множественные реплики** | Session Affinity (Sticky Sessions) | SSE соединение привязано к pod core-app |

---

## Метрики для мониторинга

1. **osago-aggregator:**
   - Latency к страховым компаниям (p50, p95, p99)
   - Timeout rate по компаниям
   - Circuit Breaker state (closed/open/half-open)
   - Retry rate

2. **core-app:**
   - Активные SSE соединения
   - gRPC stream duration
   - Rate limiting rejections

3. **Redis:**
   - Memory usage
   - Pub/Sub channel subscribers
   - Commands/sec

4. **Business metrics:**
   - Время от заявки до первого предложения (Time To First Offer)
   - Conversion rate (% пользователей, получивших хотя бы 1 предложение)
   - Среднее количество предложений на заявку

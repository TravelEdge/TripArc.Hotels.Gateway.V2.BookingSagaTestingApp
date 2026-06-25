# Booking Saga Test App — Design Spec

**Date:** 2026-06-19  
**Project:** `TripArc.Hotels.Gateway.V2.BookingSagaTestingApp`  
**Goal:** Console app that simulates the full Sabre hotel shopping flow via HGW HTTP, then triggers the Booking Saga directly via Azure Service Bus (NServiceBus), and receives the saga outcome event.

---

## Problem

The `POST /hotel/Saga/StartBooking` HTTP endpoint is `[Authorize]`-gated and returns 202 immediately. Developers need a way to test the end-to-end saga from shopping through booking confirmation by sending the `StartBookingSaga` command directly to the Service Bus queue, bypassing the HTTP layer, and observing the resulting `BookingSagaCompleted` or `BookingSagaFailed` event.

---

## Architecture

Single-file console app (`Program.cs`). No project hierarchy, no DI host — raw NServiceBus endpoint + HotelGatewayClient HTTP client.

```
Program.cs
  ├── Constants block (developer fills in before run)
  ├── Dynamic date generation (next Monday + 7 days)
  ├── HttpClient + HotelGatewayClient setup
  ├── NServiceBus endpoint setup
  ├── Shopping flow (HTTP)
  │   ├── [1/4] GetAvailability  → pick first Sabre hotel
  │   ├── [2/4] GetHotelDetails  → pick first Sabre rate
  │   └── [3/4] RoomDetails      → confirm rate
  ├── [4/4] Send StartBookingSaga via NServiceBus
  ├── Await saga outcome (5-min timeout)
  ├── Helpers: NextMonday(), BuildBookingRequest()
  └── Inline handlers: BookingSagaCompletedHandler, BookingSagaFailedHandler
```

---

## NuGet Packages (versions match HGW)

| Package | Version |
|---|---|
| `TripArc.Hotels.Gateway.V2.Client` | 1.0.1936 |
| `NServiceBus` | 9.2.8 |
| `NServiceBus.Transport.AzureServiceBus` | 5.1.2 |
| `NServiceBus.Persistence.NonDurable` | 2.0.1 |

---

## Constants Block

```csharp
// ===== CONFIGURE BEFORE RUNNING =====
const string HgwBaseUrl          = "https://hgw-dev.triparc.com";
const string BearerToken         = "eyJhbGci...";
const string NsbConnectionString = "Endpoint=sb://...";
const int    ContextId           = 2;
const string Currency            = "USD";
const string AgentCountry        = "CA";
// =====================================
```

Developer pastes values before running. Dates are computed at runtime (next Monday → +7 days).

---

## NServiceBus Configuration

- **Endpoint name:** `TripArc.HotelGateway.BookingSagaTest`
- **Transport:** `AzureServiceBusTransport` with `TopicTopology.FromOptions(new TopologyOptions())` — identical to HGW
- **Persistence:** `NonDurablePersistence` (no saga state in this app)
- **Serializer:** `SystemJsonSerializer` (must match HGW)
- **Conventions:** `IMessageBusCommand` / `IMessageBusEvent` (same as HGW)
- **Routing:** `StartBookingSaga` → `TripArc.HotelGateway.Booking`
- **DI:** `sagaResult` (`TaskCompletionSource<object>`) registered as singleton so inline handlers can signal main thread

---

## Shopping Flow (HTTP)

All steps print progress to console. Steps 1–3 each filter for `HotelProvider == "Sabre"`.

1. `GET /Hotel/Availability` — New York geo (lat 40.7128, lon -74.0060, radius 20), 1 adult, dynamic dates
2. `GET /Hotel/Availability/HotelRates` — for selected `TripArcHotelId`
3. `GET /Hotel/Availability/RoomDetails` — for selected `TripArcRateId`

If any step finds no Sabre result, the app prints a clear message and exits.

---

## StartBookingSaga Payload

Built from the hardcoded Postman payload (traveler: Ishan Soni, payment: VI 4012000098765439, pnrDataElements as provided). `token`, `checkInDate`, `checkOutDate`, `TripArcHotelId`, `TripArcRateId` come from the shopping flow results.

---

## Saga Outcome

`BookingSagaCompletedHandler` and `BookingSagaFailedHandler` are inline classes at the bottom of `Program.cs`. They receive `sagaResult` via NServiceBus DI and call `TrySetResult()`. Main thread awaits `sagaResult.Task` with a 5-minute timeout, then prints the full result and stops the endpoint.

---

## Console Output (all steps logged)

```
=== Booking Saga Test ===
Check-in: 2026-06-23  |  Check-out: 2026-06-30

[1/4] Getting availability (New York)...
  ✓ Selected Sabre hotel: <TripArcHotelId>

[2/4] Getting hotel rates...
  ✓ Selected Sabre rate: <TripArcRateId>

[3/4] Getting room details...
  ✓ Rate confirmed: <TripArcRateId>

[4/4] Sending StartBookingSaga to Service Bus...
  ✓ Sent. CorrelationId: <guid>

Waiting for saga outcome (timeout: 5 min)...

=== SAGA COMPLETED ===
Confirmation: <ConfirmationNumber>
VCC Used: false
Completed at: <timestamp>
```

Or on failure:
```
=== SAGA FAILED ===
Error: <ErrorMessage>
Code: <ErrorCode>
Compensated steps: PreBook, ProviderBook
```

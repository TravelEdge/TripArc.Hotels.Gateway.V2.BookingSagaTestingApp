using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using NServiceBus;
using NServiceBus.Transport.AzureServiceBus;
using TripArc.Core.MessageBus;
using TripArc.Hotels.Gateway.V2.Client;
using TripArc.Hotels.Gateway.V2.Shared.Dtos.Requests;
using TripArc.Hotels.Gateway.V2.Shared.Dtos.Shared;
using TripArc.Hotels.Gateway.V2.Shared.Enums;
using TripArc.Hotels.Gateway.V2.Shared.Messages.Commands.Booking;
using BookingSagaCompleted = TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaCompleted;
using BookingSagaFailed    = TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaFailed;
using TripArc.Hotels.Gateway.V2.Shared.Models;

Console.OutputEncoding = System.Text.Encoding.UTF8;
// ===== CONFIGURE BEFORE RUNNING =====
const string HgwBaseUrl          = "https://hotelsgatewayv2-api.dev.triparcdev.com";
const string BearerToken         = "*"; // replace with valid JWT before running
const string NsbConnectionString = "*"; // replace with ASB connection string before running
const string SagaQueue           = "triparc.hotelgateway.booking";
const string AgentCountry        = "CA";
// =====================================

var checkIn  = GetNextMonday();
var checkOut = checkIn.AddDays(7);

Console.WriteLine("╔══════════════════════════════════════════════╗");
Console.WriteLine("║        BOOKING SAGA TEST HARNESS             ║");
Console.WriteLine("╚══════════════════════════════════════════════╝");
Console.WriteLine($"  Check-in : {checkIn:yyyy-MM-dd}");
Console.WriteLine($"  Check-out: {checkOut:yyyy-MM-dd}");
Console.WriteLine($"  HGW      : {HgwBaseUrl}");
Console.WriteLine();

// ─── HTTP client ───────────────────────────────────────────────────────────
using var http = new HttpClient { BaseAddress = new Uri(HgwBaseUrl) };
http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", BearerToken);
var hotelClient = new HotelGatewayClient(http).HotelClient;

// ─── NServiceBus ───────────────────────────────────────────────────────────
var sagaResult = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

var endpointConfig = new EndpointConfiguration("TripArc.HotelGateway.BookingSagaTest");
endpointConfig.UseSerialization<SystemJsonSerializer>();

// HGW publishes each event type to its own per-type topic.
var topologyOptions = new TopologyOptions();
topologyOptions.SubscribedEventToTopicsMap[
    "TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaCompleted"] =
    new HashSet<string> { "triparc.hotels.gateway.v2.shared.messages.events.booking.bookingsagacompleted" };
topologyOptions.SubscribedEventToTopicsMap[
    "TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaFailed"] =
    new HashSet<string> { "triparc.hotels.gateway.v2.shared.messages.events.booking.bookingsagafailed" };
var transport = new AzureServiceBusTransport(NsbConnectionString, TopicTopology.FromOptions(topologyOptions));
transport.TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
var routing = endpointConfig.UseTransport(transport);
routing.RouteToEndpoint(typeof(StartBookingSaga), SagaQueue);

endpointConfig.UsePersistence<NonDurablePersistence>();
endpointConfig.EnableInstallers();
endpointConfig.Conventions()
    .DefiningEventsAs(t => typeof(IMessageBusEvent).IsAssignableFrom(t))
    .DefiningCommandsAs(t => typeof(IMessageBusCommand).IsAssignableFrom(t));

Console.WriteLine("[NSB] Starting endpoint...");
var endpoint = await Endpoint.Start(endpointConfig);
Console.WriteLine("[NSB] Endpoint started.");

// NSB's EnableInstallers sets ForwardTo to an http:// URL that ASB silently ignores,
// so messages accumulate in the subscription instead of reaching the queue.
// Clear ForwardTo and read directly from the subscriptions via ServiceBusProcessor.
var asbAdmin = new ServiceBusAdministrationClient(NsbConnectionString);
const string TestEndpointQueue = "TripArc.HotelGateway.BookingSagaTest";
const string CompletedTopic    = "triparc.hotels.gateway.v2.shared.messages.events.booking.bookingsagacompleted";
const string FailedTopic       = "triparc.hotels.gateway.v2.shared.messages.events.booking.bookingsagafailed";
await FixSubscription(asbAdmin, CompletedTopic, TestEndpointQueue,
    "TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaCompleted");
await FixSubscription(asbAdmin, FailedTopic, TestEndpointQueue,
    "TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking.BookingSagaFailed");

var procOpts = new ServiceBusProcessorOptions { AutoCompleteMessages = false, MaxConcurrentCalls = 1 };
await using var subClient     = new ServiceBusClient(NsbConnectionString);
await using var completedProc = subClient.CreateProcessor(CompletedTopic, TestEndpointQueue, procOpts);
await using var failedProc    = subClient.CreateProcessor(FailedTopic,    TestEndpointQueue, procOpts);

var jsonOpts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

completedProc.ProcessMessageAsync += async args =>
{
    sagaResult.TrySetResult(args.Message.Body.ToObjectFromJson<BookingSagaCompleted>(jsonOpts)!);
    await args.CompleteMessageAsync(args.Message);
};
completedProc.ProcessErrorAsync += args =>
{
    Console.WriteLine($"[PROC] Error: {args.Exception.Message}");
    return Task.CompletedTask;
};

failedProc.ProcessMessageAsync += async args =>
{
    sagaResult.TrySetResult(args.Message.Body.ToObjectFromJson<BookingSagaFailed>(jsonOpts)!);
    await args.CompleteMessageAsync(args.Message);
};
failedProc.ProcessErrorAsync += args =>
{
    Console.WriteLine($"[PROC] Error: {args.Exception.Message}");
    return Task.CompletedTask;
};

await completedProc.StartProcessingAsync();
await failedProc.StartProcessingAsync();
Console.WriteLine();

try
{
    // ── Step 1: Availability ─────────────────────────────────────────────
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    Console.WriteLine("[1/4] GET /Hotel/Availability  (New York, 1 adult)");
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    var token = Guid.NewGuid();
    var availRequest = new GetHotelAvailabilityRequest
    {
        Token        = token,
        CheckInDate  = checkIn.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        CheckOutDate = checkOut.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        Rooms        = [new RoomDto { TotalAdults = 1 }],
        GeoReference = new GeoPoint { Latitude = 40.7128, Longitude = -74.0060, Radius = 20.0 },
        RequestContext = new RequestContext
        {
            ContextId    = (Tenants)2,
            Currency     = CurrencyCodes.USD,
            AgentCountry = AgentCountry
        }
    };

    var availResponse = await hotelClient.GetAvailability(availRequest);
    var hotelResults  = availResponse?.Result?.HotelResults;

    if (hotelResults is null || hotelResults.Count == 0)
    {
        Console.WriteLine("  ✗ No hotels returned. Exiting.");
        return;
    }

    Console.WriteLine($"  Hotels returned  : {hotelResults.Count}");

    var sabreHotel = hotelResults.FirstOrDefault(h =>
        h.LeadInPrices?.Any(p =>
            p.Metadata?.ProviderInfos?.Any(pi => pi.HotelProvider == HotelProviders.Sabre) == true) == true);

    if (sabreHotel is null)
    {
        Console.WriteLine("  ✗ No Sabre hotel found in results. Exiting.");
        return;
    }

    Console.WriteLine($"  ✓ Hotel selected : {sabreHotel.TripArcHotelId} — {sabreHotel.HotelPropertyName}");
    Console.WriteLine();

    // ── Step 2: Hotel Rates ──────────────────────────────────────────────
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    Console.WriteLine("[2/4] GET /Hotel/Availability/HotelRates");
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    var ratesRequest = new HotelDetailsRequest
    {
        Token          = token,
        CheckInDate    = checkIn.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        CheckOutDate   = checkOut.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        TripArcHotelId = sabreHotel.TripArcHotelId,
        Rooms          = [new RoomDto { TotalAdults = 1 }],
        RequestContext = new RequestContext
        {
            ContextId    = (Tenants)2,
            Currency     = CurrencyCodes.USD,
            AgentCountry = AgentCountry
        }
    };

    var ratesResponse = await hotelClient.GetHotelDetails(ratesRequest);
    var rooms         = ratesResponse?.Result?.Rooms;

    if (rooms is null || rooms.Count == 0)
    {
        Console.WriteLine("  ✗ No rooms returned. Exiting.");
        return;
    }

    Console.WriteLine($"  Rooms returned   : {rooms.Count}");

    var sabreRoom = rooms.FirstOrDefault(r => r.Metadata?.ProviderInfo?.HotelProvider == HotelProviders.Sabre);

    if (sabreRoom is null)
    {
        Console.WriteLine("  ✗ No Sabre room found. Exiting.");
        return;
    }

    Console.WriteLine($"  ✓ Rate selected  : {sabreRoom.TripArcRateId}");
    Console.WriteLine($"    Rate name      : {sabreRoom.HotelRateName}");
    Console.WriteLine($"    Total price    : {sabreRoom.TotalPrice?.Amount} {sabreRoom.TotalPrice?.CurrencyCode}");
    Console.WriteLine($"    Refundable     : {sabreRoom.Metadata?.RefundableRate?.ToString() ?? "Unknown"}");
    Console.WriteLine();

    // ── Step 3: Room Details ─────────────────────────────────────────────
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    Console.WriteLine("[3/4] GET /Hotel/Availability/RoomDetails");
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    var roomDetailRequest = new RoomDetailRequest
    {
        Token          = token,
        CheckInDate    = checkIn.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        CheckOutDate   = checkOut.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        TripArcHotelId = sabreHotel.TripArcHotelId,
        TripArcRateId  = sabreRoom.TripArcRateId,
        Rooms          = [new RoomDto { TotalAdults = 1 }],
        RequestContext = new RequestContext
        {
            ContextId    = (Tenants)2,
            Currency     = CurrencyCodes.USD,
            AgentCountry = AgentCountry
        }
    };

    var roomDetailResponse = await hotelClient.RoomDetails(roomDetailRequest);
    var confirmedRoom      = roomDetailResponse?.Result?.Room;

    if (confirmedRoom is null)
    {
        Console.WriteLine("  ✗ Room details not available. Exiting.");
        return;
    }

    Console.WriteLine($"  ✓ Rate confirmed : {confirmedRoom.TripArcRateId}");
    Console.WriteLine($"    Cancellation   : {confirmedRoom.CancellationTerms ?? "N/A"}");
    Console.WriteLine($"    Book req.      : {confirmedRoom.HotelBookRequirement}");
    Console.WriteLine();

    // ── Step 4: Send StartBookingSaga ────────────────────────────────────
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    Console.WriteLine("[4/4] Sending StartBookingSaga → Azure Service Bus");
    Console.WriteLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    var correlationId = Guid.NewGuid();
    var externalRef   = $"TEST{Random.Shared.Next(1000000000, int.MaxValue)}";

    var bookingRequest = BuildBookingRequest(
        token, checkIn, checkOut,
        sabreHotel.TripArcHotelId, confirmedRoom.TripArcRateId,
        externalRef, AgentCountry,
        confirmedRoom.HotelBookRequirement);

    var command = new StartBookingSaga
    {
        BookingCorrelationId = correlationId,
        CorrelationId        = correlationId.ToString(),
        BookingRequest       = bookingRequest,
        Metadata = new Dictionary<string, string>
        {
            [BookingSagaMetadataKeys.ApplicationId] = "BookingSagaTest",
            [BookingSagaMetadataKeys.TripId]        = correlationId.ToString(),
            [BookingSagaMetadataKeys.ServiceId]     = correlationId.ToString()
        }
    };

    var sendOptions = new SendOptions();
    sendOptions.SetDestination(SagaQueue);
    await endpoint.Send(command, sendOptions);

    Console.WriteLine($"  ✓ Command sent");
    Console.WriteLine($"    CorrelationId  : {correlationId}");
    Console.WriteLine($"    ExternalRef    : {externalRef}");
    Console.WriteLine($"    Hotel          : {sabreHotel.TripArcHotelId}");
    Console.WriteLine($"    Rate           : {confirmedRoom.TripArcRateId}");
    Console.WriteLine();
    Console.WriteLine("  Waiting for saga outcome (timeout: 3 min)...");

    // ── Step 5: Await saga outcome ───────────────────────────────────────
    var result = await sagaResult.Task.WaitAsync(TimeSpan.FromMinutes(3));

    Console.WriteLine();
    switch (result)
    {
        case BookingSagaCompleted completed:
            Console.WriteLine("╔══════════════════════════════════════════════╗");
            Console.WriteLine("║           SAGA COMPLETED ✓                   ║");
            Console.WriteLine("╚══════════════════════════════════════════════╝");
            Console.WriteLine($"  Confirmation   : {completed.BookingResponse?.ConfirmationNumber ?? "N/A"}");
            Console.WriteLine($"  Itinerary ID   : {completed.BookingResponse?.ItineraryId ?? "N/A"}");
            Console.WriteLine($"  GDS Segment    : {completed.BookingResponse?.Metadata?.GdsSegmentReference ?? "N/A"}");
            Console.WriteLine($"  VCC Used       : {completed.VccInfo?.HasVcc}");
            Console.WriteLine($"  Completed At   : {completed.CompletedAt:O}");
            break;

        case BookingSagaFailed failed:
            Console.WriteLine("╔══════════════════════════════════════════════╗");
            Console.WriteLine("║             SAGA FAILED ✗                    ║");
            Console.WriteLine("╚══════════════════════════════════════════════╝");
            Console.WriteLine($"  Error          : {failed.ErrorMessage}");
            Console.WriteLine($"  Error Code     : {failed.ErrorCode ?? "N/A"}");
            Console.WriteLine($"  Failed At      : {failed.FailedAt:O}");
            if (failed.CompensatedSteps.Count > 0)
                Console.WriteLine($"  Compensated    : {string.Join(", ", failed.CompensatedSteps)}");
            break;
    }
}
catch (TimeoutException)
{
    Console.WriteLine();
    Console.WriteLine("  ✗ Timed out waiting for saga outcome (3 minutes elapsed).");
}
catch (Exception ex)
{
    Console.WriteLine($"  ✗ {ex.GetType().Name}: {ex.Message}");
}
finally
{
    Console.WriteLine();
    Console.WriteLine("[NSB] Stopping endpoint...");
    await endpoint.Stop();
    Console.WriteLine("[NSB] Done.");
}

// ─── Helpers ───────────────────────────────────────────────────────────────

static DateOnly GetNextMonday()
{
    var today = DateOnly.FromDateTime(DateTime.Today);
    int days  = ((int)DayOfWeek.Monday - (int)today.DayOfWeek + 7) % 7;
    return days == 0 ? today.AddDays(7) : today.AddDays(days);
}

static BookingRequest BuildBookingRequest(
    Guid token, DateOnly checkIn, DateOnly checkOut,
    string hotelId, string rateId,
    string externalRef, string agentCountry,
    BookRequirements bookRequirement)
{
    return new BookingRequest
    {
        Token                = token,
        TripArcHotelId       = hotelId,
        TripArcRateId        = rateId,
        CheckInDate          = checkIn.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        CheckOutDate         = checkOut.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        HotelBookRequirement = bookRequirement,
        RequestContext = new BookingRequestContext
        {
            ContextId           = (Tenants)2,
            ExternalReferenceId = externalRef,
            AdvisorInfo = new BookingAdvisorInfo
            {
                Name      = "Ishan Soni",
                Phone     = "2125457460",
                Email     = "ishan.soni@trip-arc.com",
                AdvisorId = 17899,
                OfficeInfo = new OfficeInfo
                {
                    Id       = 204,
                    Name     = "Ensemble Travel Group",
                    Address1 = "256 West 38th Street, 11th Floor",
                    City     = "Toronto",
                    Zip      = "10018",
                    State    = "Toronto",
                    Country  = agentCountry
                }
            }
        },
        Rooms =
        [
            new PNRRoomDto
            {
                Travelers =
                [
                    new BookingTraveler
                    {
                        ReferenceNumber  = "1",
                        Title            = "Mr.",
                        FirstName        = "Ishan",
                        LastName         = "Soni",
                        IsBookingContact = true,
                        Phone            = "6474671196",
                        Email            = "ishan.soni@trip-arc.com",
                        IsChild          = false,
                        LoyaltyCards     = [],
                        DateOfBirth      = new DateTime(1996, 11, 18, 0, 0, 0, DateTimeKind.Utc)
                    }
                ]
            }
        ],
        PaymentInfo = new PaymentInfo
        {
            CreditCardDetails = new CreditCardDto
            {
                Number           = "4012000098765439",
                Type             = CreditCardTypes.VI,
                VerificationCode = "123",
                ExpirationMonth  = 12,
                ExpirationYear   = 2030,
                CreditCardHolder = new CreditCardHolderDto
                {
                    FirstName   = "Test",
                    LastName    = "Run",
                    Email       = "noreply@trip-arc.com",
                    City        = "Toronto",
                    CountryCode = "CA",
                    StateCode   = "ON",
                    Address1    = "1 Test St"
                }
            }
        },
        PnrDataElements =
        [
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)2,  Value = "GPWVT" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)10, Value = "UD800 aDX" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)29, Value = "UD223 HotelMicroservice" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)30, Value = "UD227 4318948" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)28, Value = "39031" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)31, Value = "UD911 18KFFINTEST" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)4,  Value = "48" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)5,  Value = "W2" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)7,  Value = "TTN206" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)6,  Value = "6474671196" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)8,  Value = "6474671196" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)9,  Value = "sample@trip-arc.com" },
            new PnrDataElementDto { PnrDataElementType = (PnrDataElementTypes)24, Value = "18KFFINTEST" }
        ]
    };
}

// ─── ASB subscription helper ───────────────────────────────────────────────

static async Task FixSubscription(
    ServiceBusAdministrationClient admin,
    string topic, string subscription, string typeFullName)
{
    var subProps = (await admin.GetSubscriptionAsync(topic, subscription)).Value;

    // NSB sets ForwardTo to an http:// URL that ASB silently ignores. Clear it so
    // ServiceBusProcessor can read directly from the subscription.
    if (!string.IsNullOrEmpty(subProps.ForwardTo))
    {
        subProps.ForwardTo = string.Empty;
        await admin.UpdateSubscriptionAsync(subProps);
    }

    // Replace NSB's assembly-qualified filter with a FullName-only LIKE so
    // HGW's EnclosedMessageTypes header (different assembly) still matches.
    var rules = new List<RuleProperties>();
    await foreach (var r in admin.GetRulesAsync(topic, subscription))
        rules.Add(r);
    foreach (var r in rules)
        await admin.DeleteRuleAsync(topic, subscription, r.Name);

    var filter = $"[NServiceBus.EnclosedMessageTypes] LIKE '%{typeFullName}%'";
    await admin.CreateRuleAsync(topic, subscription,
        new CreateRuleOptions("NsbMessageTypeFilter", new SqlRuleFilter(filter)));
}

// ─── Locally defined message types ─────────────────────────────────────────
// TripArc.Hotels.Gateway.V2.Shared NuGet does not export the Messages namespace.
// These mirror HGW's internal types so the FullName matches EnclosedMessageTypes.

namespace TripArc.Hotels.Gateway.V2.Shared.Messages.Commands.Booking
{
    using TripArc.Core.MessageBus;
    using TripArc.Hotels.Gateway.V2.Shared.Dtos.Requests;

    public class StartBookingSaga : IMessageBusCommand
    {
        public string CorrelationId { get; set; }
        public Guid BookingCorrelationId { get; set; }
        public BookingRequest BookingRequest { get; set; }
        public Dictionary<string, string> Metadata { get; set; } = new();
    }

    public static class BookingSagaMetadataKeys
    {
        public const string ApplicationId = "applicationId";
        public const string TripId = "tripId";
        public const string ServiceId = "serviceId";
    }
}

namespace TripArc.Hotels.Gateway.V2.Shared.Messages.Events.Booking
{
    using TripArc.Core.MessageBus;
    using TripArc.Hotels.Gateway.V2.Shared.Dtos.Responses;

    public class BookingSagaCompleted : IMessageBusEvent
    {
        public string CorrelationId { get; set; }
        public Guid BookingCorrelationId { get; set; }
        public BookingResponse BookingResponse { get; set; }
        public VccInfo VccInfo { get; set; }
        public DateTime CompletedAt { get; set; }
    }

    public class VccInfo
    {
        public bool HasVcc { get; set; }
        public string PurchaseLogUniqueId { get; set; }
    }

    public class BookingSagaFailed : IMessageBusEvent
    {
        public string CorrelationId { get; set; } = string.Empty;
        public Guid BookingCorrelationId { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public string? ErrorCode { get; set; }
        public List<string> CompensatedSteps { get; set; } = new();
        public DateTime FailedAt { get; set; }
    }
}
